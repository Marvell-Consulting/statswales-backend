// @ts-check
import eslint from '@eslint/js';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';
import eslintPluginPrettierRecommended from 'eslint-plugin-prettier/recommended';
import importPlugin from 'eslint-plugin-import';

const config = defineConfig([
  {
    // global ignores need to be in their own block otherwise they don't seem to work
    ignores: [
      '.docker/**',
      '.github/**',
      '.vscode/**',
      'coverage/**',
      'dist/**',
      'node_modules/**',
      '**/*.config.{mjs,ts}'
    ],
  },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  eslintPluginPrettierRecommended,
  importPlugin.flatConfigs.recommended,
  importPlugin.flatConfigs.typescript,
  {
    rules: {
      'no-console': 'error',
      'line-comment-position': 'off',
      'no-warning-comments': 'off', // allow todo comments
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/member-ordering': 'off',
      '@typescript-eslint/naming-convention': [
        'error',
        {
          selector: 'default',
          format: ['camelCase', 'PascalCase', 'UPPER_CASE', 'snake_case'],
          leadingUnderscore: 'allow'
        },
        {
          selector: 'import',
          format: ['camelCase', 'PascalCase'],
        },
        {
          selector: 'typeLike',
          format: ['PascalCase']
        }
      ],
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          args: 'after-used',
          argsIgnorePattern: '^_',
          caughtErrors: 'all',
          caughtErrorsIgnorePattern: '^_',
          destructuredArrayIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          ignoreRestSiblings: true
        }
      ],
      '@typescript-eslint/explicit-function-return-type': 'error',
      'import/no-deprecated': 'warn',
      'import/no-empty-named-blocks': 'error',
      'import/no-extraneous-dependencies': 'error',
      'import/no-mutable-exports': 'error',
      'import/no-unused-modules': 'error',
      'import/no-absolute-path': 'error',
      'import/no-self-import': 'error',
      'import/no-useless-path-segments': 'error',
      'import/enforce-node-protocol-usage': ['error', 'always'],
      'import/no-unresolved': ['error', { ignore: ['pechkin/dist/types.js', 'openid-client/passport', 'csv-stringify/sync'] }],
    }
  },
  {
    files: ['**/entities/**/*.ts'],
    rules: {
      'import/no-cycle': 'off'
    }
  },
  {
    files: ['src/config/**/*.ts', 'test/helpers/jest-setup.ts'],
    rules: {
      'no-process-env': 'off',
    }
  },
  {
    files: ['test/**/*.ts'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off'
    }
  }
]);

export default config;
