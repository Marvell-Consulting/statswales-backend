// @ts-check
import shopifyEslintPlugin from '@shopify/eslint-plugin';

export default [
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
  ...shopifyEslintPlugin.configs.typescript,
  ...shopifyEslintPlugin.configs.prettier,
  {
    rules: {
      'line-comment-position': 'off',
      'no-process-env': 'warn',
      'no-warning-comments': 'warn',
      '@typescript-eslint/member-ordering': 'off',
      '@typescript-eslint/naming-convention': [
        'error',
        {
          selector: 'default',
          format: ['camelCase', 'PascalCase', 'UPPER_CASE', 'snake_case'],
        }
      ],
    }
  },
  {
    files: ['**/entities/*.ts'],
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
];
