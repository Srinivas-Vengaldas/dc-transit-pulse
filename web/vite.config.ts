import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  // Recharts + Leaflet make one ~240 kB (gzip) bundle; fine for a single page.
  build: { chunkSizeWarningLimit: 1000 },
});
