type Phase =
  | "seed-write"
  | "cache-fill-read"
  | "warm-read"
  | "write"
  | "mixed"
  | "balanced-get"
  | "balanced-put"
  | "durability-drain";

type Profile = "default" | "slatedb-balanced";

type Config = {
  baseUrl: string;
  token: string;
  profile: Profile;
  databasePrefix: string;
  databases: number;
  records: number;
  valueBytes: number;
  concurrency: number;
  durationSeconds: number;
  warmupSeconds: number;
  batchSize: number;
  readPercent: number;
  output: "table" | "json";
  outputFile?: string;
};

type DatabaseStatus = {
  open: boolean;
  cache_populated: boolean;
  adapter: Record<string, number>;
  slatedb: Array<{
    name: string;
    labels: Array<[string, string]>;
    value: Record<string, unknown>;
  }>;
};

type Sample = {
  latencyMs: number;
  measurable?: boolean;
  error?: string;
};

type Result = {
  phase: Phase;
  operations: number;
  errors: number;
  seconds: number;
  operationsPerSecond: number;
  unmeasurableOperations: number;
  p1Ms: number | null;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
  p999Ms: number | null;
  maxMs: number | null;
  errorSamples: string[];
};

type BenchmarkOperation = {
  operation: "get" | "put";
  key: string;
};

type BenchmarkBatchResponse = {
  get: EmbeddedMeasurements;
  put: EmbeddedMeasurements;
};

type EmbeddedMeasurements = {
  operations: number;
  unmeasurableOperations: number;
  measurableLatencyNs: number[];
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
const telemetry: Record<string, DatabaseStatus[]> = {};

if (config.profile === "slatedb-balanced") {
  await runSlatedbBalanced();
} else {
  await runDefault();
}
await finish(results);

async function runDefault(): Promise<void> {
  results.push(
    await runCounted("seed-write", config.databases * config.records, putSeed),
  );
  if (results.at(-1)?.errors) await finish(results);

  await prepareSeededDatabases();
  await captureTelemetry("beforeWarmup");

  results.push(
    await runCounted(
      "cache-fill-read",
      config.databases * config.records,
      getSeed,
    ),
  );
  if (results.at(-1)?.errors) await finish(results);

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
}

async function runSlatedbBalanced(): Promise<void> {
  results.push(await seedBalanced());
  console.error("seed complete");
  if (results.at(-1)?.errors) await finish(results);
  await prepareSeededDatabases();
  console.error("database preparation complete");
  await captureTelemetry("beforeWarmup");

  const selectRecord = scrambledZipfianSampler(config.records, 0.99);
  await runBalancedFor(config.warmupSeconds * 1_000, selectRecord);
  console.error("warmup complete");
  await captureTelemetry("afterWarmup");

  const started = performance.now();
  const measurements = await runBalancedFor(
    config.durationSeconds * 1_000,
    selectRecord,
  );
  const elapsed = performance.now() - started;
  console.error("measurement complete");
  results.push(
    summarizeEmbedded("balanced-get", measurements.get, elapsed),
    summarizeEmbedded("balanced-put", measurements.put, elapsed),
  );
  await captureTelemetry("afterMeasurement");
  assertNoObjectReset("afterWarmup", "afterMeasurement");
  console.error("measurement telemetry captured");
  results.push(
    await runCounted("durability-drain", databases.length, async (index) => {
      await admin(databases[index], "flush");
    }),
  );
  console.error("durability drain complete");
  await captureTelemetry("afterDurabilityDrain");
}

function assertNoObjectReset(before: string, after: string): void {
  for (let index = 0; index < databases.length; index++) {
    const previous = telemetry[before]?.[index]?.adapter;
    const current = telemetry[after]?.[index]?.adapter;
    if (!previous || !current) throw new Error("benchmark telemetry is incomplete");
    for (const [counter, value] of Object.entries(previous)) {
      if ((current[counter] ?? 0) < value) {
        throw new Error(
          `Durable Object reset during measurement: ${counter} fell from ${value} to ${current[counter] ?? 0}`,
        );
      }
    }
  }
}

async function captureTelemetry(checkpoint: string): Promise<void> {
  const statuses = await Promise.all(
    databases.map((database) =>
      requestJson<DatabaseStatus>(`/v1/db/${database}/stats`),
    ),
  );
  telemetry[checkpoint] = statuses.map((status) => ({
    ...status,
    slatedb: status.slatedb.filter(reportMetric),
  }));
}

function reportMetric(metric: DatabaseStatus["slatedb"][number]): boolean {
  const value = metric.value.value ?? metric.value.count ?? 0;
  return (
    value !== 0 ||
    metric.name === "slatedb.db.backpressure_count" ||
    metric.name === "slatedb.db.l0_stall_count" ||
    metric.name === "slatedb.compactor.running_compactions"
  );
}

async function seedBalanced(): Promise<Result> {
  const started = performance.now();
  const measurements = emptyEmbeddedMeasurements();
  for (const databaseMeasurements of await Promise.all(
    databases.map(seedBalancedDatabase),
  )) {
    mergeEmbedded(measurements, databaseMeasurements);
  }
  return summarizeEmbedded(
    "seed-write",
    measurements,
    performance.now() - started,
  );
}

async function seedBalancedDatabase(
  database: string,
): Promise<EmbeddedMeasurements> {
  const measurements = emptyEmbeddedMeasurements();
  let next = 0;
  while (next < config.records) {
    const clients = Array.from({ length: config.concurrency }, () => {
      const operations: BenchmarkOperation[] = [];
      while (operations.length < config.batchSize && next < config.records) {
        operations.push({ operation: "put", key: seedKey(next++) });
      }
      return operations;
    }).filter((operations) => operations.length > 0);
    const response = await benchmarkBatch(database, clients);
    mergeEmbedded(measurements, response.put);
  }
  return measurements;
}

async function prepareSeededDatabases(): Promise<void> {
  for (const database of databases) {
    await admin(database, "flush");
    await admin(database, "reopen");
    await admin(database, "cache/clear");
  }
}

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

async function benchmarkBatch(
  database: string,
  clients: BenchmarkOperation[][],
): Promise<BenchmarkBatchResponse> {
  const response = await request(`/v1/db/${database}/admin/benchmark/batch`, {
    method: "POST",
    body: JSON.stringify({ clients, value }),
  });
  return response.json() as Promise<BenchmarkBatchResponse>;
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

async function runBalancedFor(
  durationMs: number,
  selectRecord: (random: () => number) => number,
): Promise<BenchmarkBatchResponse> {
  const deadline = performance.now() + durationMs;
  const measurements = emptyBenchmarkResponse();
  const randoms = Array.from({ length: config.concurrency }, (_, client) =>
    randomGenerator(client + 1),
  );
  while (performance.now() < deadline) {
    const clientsByDatabase = databases.map(() => [] as BenchmarkOperation[][]);
    for (const random of randoms) {
      const databaseIndex = Math.floor(random() * databases.length);
      clientsByDatabase[databaseIndex].push(
        Array.from({ length: config.batchSize }, () => ({
          operation:
            random() < config.readPercent / 100
              ? ("get" as const)
              : ("put" as const),
          key: seedKey(selectRecord(random)),
        })),
      );
    }
    const responses = await Promise.all(
      databases.flatMap((database, index) =>
        clientsByDatabase[index].length > 0
          ? [benchmarkBatch(database, clientsByDatabase[index])]
          : [],
      ),
    );
    for (const response of responses) {
      mergeEmbedded(measurements.get, response.get);
      mergeEmbedded(measurements.put, response.put);
    }
  }
  return measurements;
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
  const successfulSamples = samples.filter((entry) => !entry.error);
  const successful = successfulSamples
    .map((entry) => entry.latencyMs)
    .sort((left, right) => left - right);
  const unmeasurableOperations = successfulSamples.filter(
    (entry) => entry.measurable === false,
  ).length;
  const latency = (quantile: number): number | null =>
    unmeasurableOperations > 0 ? null : round(percentile(successful, quantile));
  const errors = samples.filter((entry) => entry.error);
  const seconds = elapsedMs / 1_000;
  return {
    phase,
    operations: samples.length,
    errors: errors.length,
    seconds: round(seconds),
    operationsPerSecond: round(samples.length / seconds),
    unmeasurableOperations,
    p1Ms: latency(0.01),
    p50Ms: latency(0.5),
    p95Ms: latency(0.95),
    p99Ms: latency(0.99),
    p999Ms: latency(0.999),
    maxMs:
      unmeasurableOperations > 0 ? null : round(successful.at(-1) ?? 0),
    errorSamples: [...new Set(errors.flatMap((entry) => entry.error ?? []))].slice(
      0,
      5,
    ),
  };
}

function summarizeEmbedded(
  phase: Phase,
  measurements: EmbeddedMeasurements,
  elapsedMs: number,
): Result {
  const latencyMs = measurements.measurableLatencyNs
    .map((latencyNs) => latencyNs / 1_000_000)
    .sort((left, right) => left - right);
  const latency = (quantile: number): number | null =>
    measurements.operations === 0 || measurements.unmeasurableOperations > 0
      ? null
      : round(percentile(latencyMs, quantile));
  const seconds = elapsedMs / 1_000;
  return {
    phase,
    operations: measurements.operations,
    errors: 0,
    seconds: round(seconds),
    operationsPerSecond: round(measurements.operations / seconds),
    unmeasurableOperations: measurements.unmeasurableOperations,
    p1Ms: latency(0.01),
    p50Ms: latency(0.5),
    p95Ms: latency(0.95),
    p99Ms: latency(0.99),
    p999Ms: latency(0.999),
    maxMs:
      measurements.operations === 0 || measurements.unmeasurableOperations > 0
        ? null
        : round(latencyMs.at(-1) ?? 0),
    errorSamples: [],
  };
}

function emptyBenchmarkResponse(): BenchmarkBatchResponse {
  return {
    get: emptyEmbeddedMeasurements(),
    put: emptyEmbeddedMeasurements(),
  };
}

function emptyEmbeddedMeasurements(): EmbeddedMeasurements {
  return {
    operations: 0,
    unmeasurableOperations: 0,
    measurableLatencyNs: [],
  };
}

function mergeEmbedded(
  target: EmbeddedMeasurements,
  source: EmbeddedMeasurements,
): void {
  target.operations += source.operations;
  target.unmeasurableOperations += source.unmeasurableOperations;
  target.measurableLatencyNs.push(...source.measurableLatencyNs);
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

function scrambledZipfianSampler(
  count: number,
  theta: number,
): (random: () => number) => number {
  const cumulative = new Float64Array(count);
  const scrambled = new Uint32Array(count);
  let total = 0;
  for (let rank = 0; rank < count; rank++) {
    total += 1 / Math.pow(rank + 1, theta);
    cumulative[rank] = total;
    scrambled[rank] = Number(fnv64(BigInt(rank)) % BigInt(count));
  }
  return (random) => {
    const target = random() * total;
    let low = 0;
    let high = cumulative.length - 1;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (cumulative[middle] < target) low = middle + 1;
      else high = middle;
    }
    return scrambled[low];
  };
}

function fnv64(input: bigint): bigint {
  let value = input;
  let hash = 14_695_981_039_346_656_037n;
  for (let byte = 0; byte < 8; byte++) {
    hash ^= value & 0xffn;
    hash = BigInt.asUintN(64, hash * 1_099_511_628_211n);
    value >>= 8n;
  }
  return hash;
}

function readConfig(): Config {
  const profile = Bun.env.BENCH_PROFILE ?? "default";
  if (profile !== "default" && profile !== "slatedb-balanced") {
    throw new Error("BENCH_PROFILE must be default or slatedb-balanced");
  }
  const balanced = profile === "slatedb-balanced";
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
    profile,
    databasePrefix,
    databases: integer("BENCH_DATABASES", 1, 1, 64),
    records: integer("BENCH_RECORDS", balanced ? 10_000 : 1_000, 1, 1_000_000),
    valueBytes: integer("BENCH_VALUE_BYTES", balanced ? 400 : 1_024, 1, 1_000_000),
    concurrency: integer("BENCH_CONCURRENCY", balanced ? 64 : 8, 1, 256),
    durationSeconds: integer(
      "BENCH_DURATION_SECONDS",
      balanced ? 900 : 15,
      1,
      3_600,
    ),
    warmupSeconds: integer(
      "BENCH_WARMUP_SECONDS",
      balanced ? 300 : 3,
      0,
      3_600,
    ),
    batchSize: integer("BENCH_BATCH_SIZE", balanced ? 32 : 1, 1, 512),
    readPercent: integer("BENCH_READ_PERCENT", 50, 0, 100),
    output,
    outputFile: Bun.env.BENCH_OUTPUT_FILE,
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

async function finish(phaseResults: Result[]): Promise<never> {
  const report = {
    generatedAt: new Date().toISOString(),
    config: { ...config, token: "<redacted>" },
    measurement:
      config.profile === "slatedb-balanced"
        ? {
            latencyBoundary: "SlateDB Db call inside the Durable Object",
            throughputBoundary: "external client over batched requests",
            deployedClock:
              "unavailable when no I/O advances the Worker clock; reported as null",
          }
        : {
            latencyBoundary: "end-to-end HTTP request",
            throughputBoundary: "external client over individual requests",
          },
    results: phaseResults,
    telemetry,
  };
  const json = JSON.stringify(report, null, 2);
  if (config.outputFile) {
    await Bun.write(config.outputFile, `${json}\n`);
  }
  if (config.output === "json") {
    console.log(json);
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
