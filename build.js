const { buildSync } = require("esbuild");

buildSync({
  entryPoints: ["src/index.ts"],
  outdir: "dist",
  bundle: true,
  minify: true,
  sourcemap: false,
  platform: "node",
  treeShaking: true,
  tsconfig: "tsconfig.json",
});
