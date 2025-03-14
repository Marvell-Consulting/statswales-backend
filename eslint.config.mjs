// @ts-check
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintPluginPrettierRecommended from 'eslint-plugin-prettier/recommended';

export default tseslint.config(
  [
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
    {
      rules: {
        'no-console': 'error',
        'line-comment-position': 'off',
        'no-warning-comments': 'warn',
        '@typescript-eslint/no-explicit-any': 'warn',
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
      }
    },
    {
      files: ['**/entities/**/*.ts'],
      rules: {
        'import/no-cycle': 'off',
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
        '@typescript-eslint/no-explicit-any': 'off'
      }
    }
  ]
);
