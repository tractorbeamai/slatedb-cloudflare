import oxfmtConfig from "@tractorbeam/oxfmt-config";
import oxlintConfig from "@tractorbeam/oxlint-config";
import { defineConfig } from "vite-plus";

const lint = oxlintConfig({ react: false });

// Vite overwrites BASE_URL in the test process with its own `base` ("/"), so
// hand the caller's value to the benchmark under another name.
const benchBaseUrl = process.env.BASE_URL;

const generatedPatterns = [
  "**/node_modules/**",
  "**/*.toml",
  ".wrangler/**",
  "**/target/**",
  "**/vendor/**",
  "durable-object/benchmarks/**",
  "container/worker-configuration.d.ts",
];

export default defineConfig({
  fmt: {
    ...oxfmtConfig,
    ignorePatterns: [...oxfmtConfig.ignorePatterns, ...generatedPatterns],
  },
  lint: {
    ...lint,
    ignorePatterns: generatedPatterns,
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    env: benchBaseUrl ? { BENCH_BASE_URL: benchBaseUrl } : {},
    disableConsoleIntercept: true,
    testTimeout: 0,
    hookTimeout: 0,
    benchmark: { include: ["scripts/**/*.bench.ts"] },
  },
});
