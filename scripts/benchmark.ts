type Phase =
  | "seed-write"
  | "cache-fill-read"
  | "warm-read"
  | "write"
  | "mixed";

type Config = {
  baseUrl: string;
  token: string;
  databasePrefix: string;
  databases: number;
  records: number;
  valueBytes: number;
  concurrency: number;
  durationSeconds: number;
  warmupSeconds: number;
  output: "table" | "json";
};

type Sample = {
  latencyMs: number;
  error?: string;
};

type Result = {
  phase: Phase;
  operations: number;
  errors: number;
  seconds: number;
  operationsPerSecond: number;
  p1Ms: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  p999Ms: number;
  maxMs: number;
  errorSamples: string[];
};

const config = readConfig();
const databases = Array.from(
  { length: config.databases },
  (_, index) => `${config.databasePrefix}-${index}`,
);
const value = "x".repeat(config.valueBytes);
const headers = {
  authorization: `Bearer ${config.token}`,
  "content-type": "application/json",
};

const results: Result[] = [];

results.push(
  await runCounted("seed-write", config.databases * config.records, putSeed),
);
if (results.at(-1)?.errors) finish(results);

for (const database of databases) {
  await admin(database, "flush");
  await admin(database, "reopen");
  await admin(database, "cache/clear");
}

results.push(
  await runCounted(
    "cache-fill-read",
    config.databases * config.records,
    getSeed,
  ),
);
if (results.at(-1)?.errors) finish(results);

for (const database of databases) {
  const status = await requestJson<{ cache_populated: boolean }>(
    `/v1/db/${database}/stats`,
  );
  if (!status.cache_populated) {
    throw new Error(`cache did not populate for ${database}`);
  }
}

results.push(await runTimed("warm-read", getRandomSeed));

let writeSequence = 0;
results.push(
  await runTimed("write", async (client) => {
    const sequence = writeSequence++;
    const database = databases[sequence % databases.length];
    await put(database, `write-${client}-${sequence}`);
  }),
);

results.push(
  await runTimed("mixed", async (client, random) => {
    if (random() < 0.5) {
      await getRandomSeed(client, random);
      return;
    }
    const sequence = writeSequence++;
    const database = databases[sequence % databases.length];
    await put(database, `mixed-${client}-${sequence}`);
  }),
);

finish(results);

async function putSeed(index: number): Promise<void> {
  const databaseIndex = index % databases.length;
  const record = Math.floor(index / databases.length);
  await put(databases[databaseIndex], seedKey(record));
}

async function getSeed(index: number): Promise<void> {
  const databaseIndex = index % databases.length;
  const record = Math.floor(index / databases.length);
  await get(databases[databaseIndex], seedKey(record));
}

async function getRandomSeed(
  _client: number,
  random: () => number,
): Promise<void> {
  const database = databases[Math.floor(random() * databases.length)];
  const record = Math.floor(random() * config.records);
  await get(database, seedKey(record));
}

async function put(database: string, key: string): Promise<void> {
  await request(`/v1/db/${database}/put`, {
    method: "POST",
    body: JSON.stringify({ key, value }),
  });
}

async function get(database: string, key: string): Promise<void> {
  const response = await requestJson<{ value: string | null }>(
    `/v1/db/${database}/get?key=${encodeURIComponent(key)}`,
  );
  if (response.value === null) throw new Error(`missing seeded key ${key}`);
}

async function admin(database: string, action: string): Promise<void> {
  await request(`/v1/db/${database}/admin/${action}`, { method: "POST" });
}

async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(`${config.baseUrl}${path}`, {
    ...init,
    headers: { ...headers, ...init.headers },
  });
  if (!response.ok) {
    const body = (await response.text()).slice(0, 200);
    throw new Error(`${response.status} ${response.statusText}: ${body}`);
  }
  return response;
}

async function requestJson<T>(path: string): Promise<T> {
  return (await request(path)).json() as Promise<T>;
}

async function runCounted(
  phase: Phase,
  count: number,
  operation: (index: number) => Promise<void>,
): Promise<Result> {
  let next = 0;
  const samples: Sample[] = [];
  const started = performance.now();
  await Promise.all(
    Array.from({ length: Math.min(config.concurrency, count) }, async () => {
      while (true) {
        const index = next++;
        if (index >= count) return;
        samples.push(await sample(() => operation(index)));
      }
    }),
  );
  return summarize(phase, samples, performance.now() - started);
}

async function runTimed(
  phase: Phase,
  operation: (client: number, random: () => number) => Promise<void>,
): Promise<Result> {
  const warmup = await runFor(config.warmupSeconds * 1_000, operation);
  const warmupError = warmup.find((entry) => entry.error)?.error;
  if (warmupError) throw new Error(`${phase} warmup failed: ${warmupError}`);
  const started = performance.now();
  const samples = await runFor(config.durationSeconds * 1_000, operation);
  return summarize(phase, samples, performance.now() - started);
}

async function runFor(
  durationMs: number,
  operation: (client: number, random: () => number) => Promise<void>,
): Promise<Sample[]> {
  const deadline = performance.now() + durationMs;
  const samples: Sample[] = [];
  await Promise.all(
    Array.from({ length: config.concurrency }, async (_, client) => {
      const random = randomGenerator(client + 1);
      while (performance.now() < deadline) {
        const result = await sample(() => operation(client, random));
        samples.push(result);
      }
    }),
  );
  return samples;
}

async function sample(operation: () => Promise<void>): Promise<Sample> {
  const started = performance.now();
  try {
    await operation();
    return { latencyMs: performance.now() - started };
  } catch (error) {
    return {
      latencyMs: performance.now() - started,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function summarize(phase: Phase, samples: Sample[], elapsedMs: number): Result {
  const successful = samples
    .filter((entry) => !entry.error)
    .map((entry) => entry.latencyMs)
    .sort((left, right) => left - right);
  const errors = samples.filter((entry) => entry.error);
  const seconds = elapsedMs / 1_000;
  return {
    phase,
    operations: samples.length,
    errors: errors.length,
    seconds: round(seconds),
    operationsPerSecond: round(samples.length / seconds),
    p1Ms: round(percentile(successful, 0.01)),
    p50Ms: round(percentile(successful, 0.5)),
    p95Ms: round(percentile(successful, 0.95)),
    p99Ms: round(percentile(successful, 0.99)),
    p999Ms: round(percentile(successful, 0.999)),
    maxMs: round(successful.at(-1) ?? 0),
    errorSamples: [...new Set(errors.flatMap((entry) => entry.error ?? []))].slice(
      0,
      5,
    ),
  };
}

function percentile(sorted: number[], quantile: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.ceil(quantile * sorted.length) - 1];
}

function randomGenerator(seed: number): () => number {
  let state = seed;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 4_294_967_296;
  };
}

function seedKey(record: number): string {
  return `seed-${record.toString().padStart(8, "0")}`;
}

function readConfig(): Config {
  const databasePrefix =
    Bun.env.BENCH_DATABASE_PREFIX ?? `bench-${Date.now()}`;
  if (!/^[A-Za-z0-9_-]+$/.test(databasePrefix) || databasePrefix.length > 110) {
    throw new Error("BENCH_DATABASE_PREFIX must be a short database-safe name");
  }
  const output = Bun.env.BENCH_OUTPUT ?? "table";
  if (output !== "table" && output !== "json") {
    throw new Error("BENCH_OUTPUT must be table or json");
  }
  return {
    baseUrl: (Bun.env.BASE_URL ?? "http://localhost:8787").replace(/\/$/, ""),
    token: required("PROBE_TOKEN"),
    databasePrefix,
    databases: integer("BENCH_DATABASES", 1, 1, 64),
    records: integer("BENCH_RECORDS", 1_000, 1, 1_000_000),
    valueBytes: integer("BENCH_VALUE_BYTES", 1_024, 1, 1_000_000),
    concurrency: integer("BENCH_CONCURRENCY", 8, 1, 256),
    durationSeconds: integer("BENCH_DURATION_SECONDS", 15, 1, 300),
    warmupSeconds: integer("BENCH_WARMUP_SECONDS", 3, 0, 60),
    output,
  };
}

function required(name: string): string {
  const value = Bun.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function integer(
  name: string,
  fallback: number,
  minimum: number,
  maximum: number,
): number {
  const value = Number(Bun.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}

function round(value: number): number {
  return Number(value.toFixed(2));
}

function finish(phaseResults: Result[]): never {
  const report = {
    generatedAt: new Date().toISOString(),
    config: { ...config, token: "<redacted>" },
    results: phaseResults,
  };
  if (config.output === "json") {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.table(
      phaseResults.map(({ errorSamples: _errorSamples, ...result }) => result),
    );
    for (const result of phaseResults) {
      if (result.errorSamples.length) {
        console.error(`${result.phase} errors:`, result.errorSamples);
      }
    }
  }
  process.exit(phaseResults.some((result) => result.errors > 0) ? 1 : 0);
}
