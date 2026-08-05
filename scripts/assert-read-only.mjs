import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = fileURLToPath(new URL("../src/", import.meta.url));
const forbidden = [
  { label: "authenticated Polymarket client", pattern: /@polymarket\/(?:clob-client|builders)/iu },
  { label: "wallet or signing library", pattern: /(?:ethers|wagmi|viem|private[_-]?key)/iu },
  { label: "credential access", pattern: /process\.env|api[_-]?key|secret[_-]?key/iu },
  { label: "mutating HTTP method", pattern: /method\s*:\s*["'](?:POST|PUT|PATCH|DELETE)["']/u },
  { label: "order endpoint", pattern: /(?:\/order|\/cancel|\/relayer)(?:s|\b|\/)/iu },
];

async function listFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const location = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await listFiles(location)));
    else if (entry.isFile() && location.endsWith(".ts")) files.push(location);
  }
  return files;
}

const failures = [];
for (const file of await listFiles(sourceRoot)) {
  const source = await readFile(file, "utf8");
  for (const rule of forbidden) {
    if (rule.pattern.test(source)) failures.push(`${file}: ${rule.label}`);
  }
}

if (failures.length > 0) {
  console.error("Read-only boundary check failed:\n" + failures.join("\n"));
  process.exitCode = 1;
} else {
  console.log("Read-only boundary check passed.");
}
