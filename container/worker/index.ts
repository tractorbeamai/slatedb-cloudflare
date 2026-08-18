import { Container, getContainer } from "@cloudflare/containers";

interface Env extends Cloudflare.Env {
  PROBE_TOKEN: string;
  R2_ACCOUNT_ID: string;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
}

export class SlateDbContainer extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "10m";
  enableInternet = true;

  envVars: Record<string, string> = {
    R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID,
    R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY,
    R2_BUCKET: this.env.R2_BUCKET,
    RUST_LOG: "slatedb_cloudflare_container=info,slatedb=info",
  };
}

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
    return getContainer(env.SLATEDB_CONTAINERS, database).fetch(request);
  },
} satisfies ExportedHandler<Env>;
