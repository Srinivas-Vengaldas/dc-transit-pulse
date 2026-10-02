import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";

import { DEFAULT_RUSH, headline, type Manifest, parseCsv, type RouteHour, type RouteSummary } from "./src/data";

const DATA = resolve(__dirname, "public/data");

const escape = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/**
 * Bake the headline into index.html: the meta description (link previews) and a static
 * summary inside #root that React replaces on load, so crawlers and readers without
 * JavaScript still see the result. Same function and counts as the page itself.
 */
function staticSummary(): Plugin {
  return {
    name: "static-summary",
    transformIndexHtml(html) {
      if (!existsSync(resolve(DATA, "manifest.json"))) return html;
      const manifest = JSON.parse(readFileSync(resolve(DATA, "manifest.json"), "utf8")) as Manifest;
      const read = <T,>(name: string): T[] => {
        const file = resolve(DATA, `${name}.csv`);
        return existsSync(file) ? parseCsv<T>(readFileSync(file, "utf8")) : [];
      };
      const lines = headline(
        read<RouteHour>("route_hour"),
        read<RouteSummary>("route_summary"),
        manifest.definitions?.rush_hours ?? DEFAULT_RUSH,
      );
      if (!lines.length) return html;
      const dates = manifest.service_dates;
      const span = dates ? ` Service dates ${dates.first_service_date} to ${dates.last_service_date}.` : "";
      const text = escape(lines.join(" ") + span);
      return html
        .replace(/(<meta name="description" content=")[^"]*(")/, `$1${escape(lines[0])} ${escape(lines[1] ?? "")}$2`)
        .replace(
          '<div id="root"></div>',
          `<div id="root"><main class="static-summary"><h1>DC Transit Pulse</h1><p>${text}</p></main></div>`,
        );
    },
  };
}

export default defineConfig({
  plugins: [react(), staticSummary()],
  // Recharts + Leaflet make one ~240 kB (gzip) bundle; fine for a single page.
  build: { chunkSizeWarningLimit: 1000 },
});
