import { Container, getContainer } from "@cloudflare/containers";

export { ContainerProxy } from "@cloudflare/containers";

interface Env extends Cloudflare.Env {
  PROBE_TOKEN: string;
  SLATEDB_BUCKET: R2Bucket;
}

interface MultipartPart {
  partNumber: number;
  etag: string;
}

function objectHeaders(object: R2Object): Headers {
  const headers = new Headers({
    etag: object.httpEtag,
    "x-slate-version": object.version,
    "x-slate-size": String(object.size),
    "x-slate-uploaded": object.uploaded.toISOString(),
  });
  if (object.range && "offset" in object.range && object.range.offset !== undefined) {
    headers.set("x-slate-range-offset", String(object.range.offset));
  }
  return headers;
}

function objectPath(request: Request): string {
  const path = request.headers.get("x-slate-path");
  if (!path) throw new Error("missing x-slate-path");
  return path;
}

async function handleR2(request: Request, bucket: R2Bucket): Promise<Response> {
  try {
    const operation = request.headers.get("x-slate-operation") ?? "object";
    if (operation === "list") {
      const url = new URL(request.url);
      const result = await bucket.list({
        prefix: url.searchParams.get("prefix") ?? undefined,
        cursor: url.searchParams.get("cursor") ?? undefined,
        delimiter: url.searchParams.get("delimiter") ?? undefined,
      });
      return Response.json({
        objects: result.objects.map((object) => ({
          key: object.key,
          size: object.size,
          etag: object.httpEtag,
          version: object.version,
          uploaded: object.uploaded.toISOString(),
        })),
        prefixes: result.delimitedPrefixes,
        cursor: result.truncated ? result.cursor : undefined,
      });
    }

    const key = objectPath(request);
    if (operation === "multipart-start") {
      const upload = await bucket.createMultipartUpload(key);
      return Response.json({ uploadId: upload.uploadId });
    }
    if (operation === "multipart-part") {
      const uploadId = request.headers.get("x-slate-upload-id");
      const partNumber = Number(request.headers.get("x-slate-part-number"));
      if (!uploadId || !Number.isInteger(partNumber)) {
        return new Response("invalid multipart part", { status: 400 });
      }
      const part = await bucket
        .resumeMultipartUpload(key, uploadId)
        .uploadPart(partNumber, await request.arrayBuffer());
      return Response.json(part);
    }
    if (operation === "multipart-complete") {
      const uploadId = request.headers.get("x-slate-upload-id");
      if (!uploadId) return new Response("missing upload id", { status: 400 });
      const parts = (await request.json()) as MultipartPart[];
      const object = await bucket.resumeMultipartUpload(key, uploadId).complete(parts);
      return new Response(null, { headers: objectHeaders(object) });
    }
    if (operation === "multipart-abort") {
      const uploadId = request.headers.get("x-slate-upload-id");
      if (!uploadId) return new Response("missing upload id", { status: 400 });
      await bucket.resumeMultipartUpload(key, uploadId).abort();
      return new Response(null, { status: 204 });
    }

    if (request.method === "HEAD") {
      const object = await bucket.head(key);
      return object
        ? new Response(null, { headers: objectHeaders(object) })
        : new Response(null, { status: 404 });
    }
    if (request.method === "GET") {
      const object = await bucket.get(key, {
        onlyIf: request.headers,
        range: request.headers,
      });
      if (!object) return new Response(null, { status: 404 });
      if (!("body" in object)) {
        const notModified =
          request.headers.has("if-none-match") || request.headers.has("if-modified-since");
        return new Response(null, {
          status: notModified ? 304 : 412,
          headers: objectHeaders(object),
        });
      }
      return new Response(object.body, { headers: objectHeaders(object) });
    }
    if (request.method === "PUT") {
      const object = await bucket.put(key, await request.arrayBuffer(), {
        onlyIf: request.headers,
      });
      return object
        ? new Response(null, { headers: objectHeaders(object) })
        : new Response(null, { status: 412 });
    }
    if (request.method === "DELETE") {
      await bucket.delete(key);
      return new Response(null, { status: 204 });
    }
    return new Response("unsupported R2 bridge request", { status: 405 });
  } catch (error) {
    console.error("R2 bridge failed", error);
    return new Response(error instanceof Error ? error.message : String(error), { status: 500 });
  }
}

export class SlateDbContainer extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "10m";
  envVars: Record<string, string> = {
    RUST_LOG: "slatedb_cloudflare_container=info,slatedb=info",
  };
}

SlateDbContainer.outboundByHost = {
  "slatedb.r2": (request: Request, env: Env) => handleR2(request, env.SLATEDB_BUCKET),
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return Response.json({ ok: true, service: "slatedb-cloudflare-container" });
    }
    if (request.headers.get("authorization") !== `Bearer ${env.PROBE_TOKEN}`) {
      return new Response("unauthorized", { status: 401 });
    }
    const match = url.pathname.match(/^\/v1\/db\/([^/]+)\//);
    if (!match) return new Response("route not found", { status: 404 });
    const database = decodeURIComponent(match[1]);
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(database)) {
      return new Response("invalid database name", { status: 400 });
    }
    return await getContainer(env.SLATEDB_CONTAINERS, database).fetch(request);
  },
} satisfies ExportedHandler<Env>;
