import oxfmtConfig from "@tractorbeam/oxfmt-config";
import oxlintConfig from "@tractorbeam/oxlint-config";
import { defineConfig } from "vite-plus";

const lint = oxlintConfig({ react: false });

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
});
