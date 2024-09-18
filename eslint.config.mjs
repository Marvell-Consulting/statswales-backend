// @ts-check
import tseslint from 'typescript-eslint';
import globals from 'globals';
import eslint from '@eslint/js';
import eslintConfigPrettier from 'eslint-config-prettier';

export default tseslint.config(
  {
    // global ignores need to be in their own config block otherwise they don't seem to work
    ignores: [
      '.docker/**',
      '.github/**',
      '.vscode/**',
      'coverage/**',
      'dist/**',
      'node_modules/**',
    ],
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      ecmaVersion: 6,
      globals: {
        ...globals.node,
      }
    },
    extends: [
      eslint.configs.recommended,
      ...tseslint.configs.recommended,
      eslintConfigPrettier,
    ],
    rules: {
      'no-console': 'error'
    }
  }
);
