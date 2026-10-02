// Copy the exported gold snapshot (../dashboard/snapshot) into public/data so Vite serves it.
// The snapshot is committed once, under dashboard/; this keeps both UIs on the same files.
// A missing snapshot is not an error: the site builds and shows a "no data yet" message.
import { copyFileSync, existsSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const source = process.env.SNAPSHOT_DIR ?? join(here, "..", "..", "dashboard", "snapshot");
const target = join(here, "..", "public", "data");

mkdirSync(target, { recursive: true });
if (!existsSync(source)) {
  console.warn(`snapshot: ${source} not found; building without data`);
} else {
  const files = readdirSync(source).filter((f) => f.endsWith(".csv") || f === "manifest.json");
  for (const f of files) copyFileSync(join(source, f), join(target, f));
  console.log(`snapshot: copied ${files.length} files from ${source}`);
}
