import type { Config } from 'jest';
import { createJsWithTsPreset } from 'ts-jest';

const config: Config = {
  ...createJsWithTsPreset({ tsconfig: 'tsconfig.spec.json' }),
  // openid-client and it's deps are now published as ESM and need transpiling to CJS
  transformIgnorePatterns: ['/node_modules/(?!(openid-client|oauth4webapi|jose)/)'],
  verbose: true,
  reporters: ['default', ['jest-junit', { outputDirectory: 'coverage/test-report', outputName: 'junit-report.xml' }]],
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
