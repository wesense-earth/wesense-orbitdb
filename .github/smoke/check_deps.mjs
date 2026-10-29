// Smoke test: every runtime dependency in package.json must still import.
// Run inside the built image (node --input-type=module -). Does not start the service.
import { readFileSync } from "node:fs";

const pkg = JSON.parse(readFileSync("/app/package.json", "utf8"));
const names = Object.keys(pkg.dependencies ?? {});
const failed = [];

for (const name of names) {
  try {
    await import(name);
    console.log(`ok   ${name}`);
  } catch (err) {
    console.log(`FAIL ${name}: ${err.code ?? err.name}: ${err.message.split("\n")[0]}`);
    failed.push(name);
  }
}

if (failed.length) {
  console.error(`${failed.length} dependencies failed to import: ${failed.join(", ")}`);
  process.exit(1);
}
console.log(`all ${names.length} dependencies imported`);
process.exit(0);
