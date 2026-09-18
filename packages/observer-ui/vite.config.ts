import tailwindcss from "@tailwindcss/postcss"
import { defineConfig } from "vite"

export default defineConfig({
    base: "./",
    css: { postcss: { plugins: [tailwindcss()] } },
    server: { host: "127.0.0.1", proxy: { "/api/observe": { target: process.env.OBSERVER_API_URL ?? "http://127.0.0.1:4174", changeOrigin: true } } },
    build: {
        outDir: "dist/standalone",
        emptyOutDir: true,
        rolldownOptions: { output: { entryFileNames: "app.js", assetFileNames: "app.[ext]" } }
    }
})
