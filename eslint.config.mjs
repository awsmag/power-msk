// @ts-check

import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettier, { rules } from "eslint-config-prettier";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ["dist", "node_modules", "build.js"],
    extends: [prettier],
  }, 

);