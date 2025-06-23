import type { Config } from 'jest';

const config: Config = {
  transform: { '^.+\\.(t|j)s$': ['ts-jest', { tsconfig: 'tsconfig.spec.json' }] },
  verbose: true,
  reporters: ['default', ['jest-junit', { outputDirectory: 'coverage/test-report', outputName: 'junit-report.xml' }]],
  preset: 'ts-jest',
  testEnvironment: 'node',
  coverageDirectory: './coverage',
  collectCoverage: true,
  coverageReporters: ['cobertura', 'lcov', 'html', 'text'],
  coveragePathIgnorePatterns: [
    '/node_modules',
    '/test/',
    '/src/migrations',
    '/src/controllers/auth.ts',
    'src/middleware/passport-auth.ts'
  ],
  setupFiles: ['<rootDir>/test/helpers/jest-setup.ts'],
  maxWorkers: 1, // TODO: temporary solution to test parallelism issue
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.ts'],
};

export default config;
