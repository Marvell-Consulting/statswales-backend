import type { Config } from 'jest';
import { createJsWithTsPreset } from 'ts-jest';

const sharedPreset = createJsWithTsPreset({ tsconfig: 'tsconfig.spec.json' });

const sharedConfig = {
  ...sharedPreset,
  // openid-client and its deps are now published as ESM and need transpiling to CJS
  transformIgnorePatterns: ['/node_modules/(?!(openid-client|oauth4webapi|jose|nanoid)/)'],
  testEnvironment: 'node' as const,
  coveragePathIgnorePatterns: [
    '/node_modules',
    '/test/',
    '/src/migrations',
    '/src/controllers/auth.ts',
    'src/middleware/passport-auth.ts'
  ]
};

const config: Config = {
  verbose: true,
  reporters: ['default', ['jest-junit', { outputDirectory: 'coverage/test-report', outputName: 'junit-report.xml' }]],
  coverageDirectory: './coverage',
  collectCoverage: true,
  coverageReporters: ['cobertura', 'lcov', 'html', 'text'],
  coverageThreshold: {
    global: {
      statements: 61,
      branches: 49,
      functions: 57,
      lines: 61
    }
  },
  projects: [
    {
      ...sharedConfig,
      displayName: 'unit',
      setupFiles: ['<rootDir>/test/helpers/jest-setup.ts'],
      roots: ['<rootDir>/test'],
      testMatch: ['<rootDir>/test/unit/**/*.test.ts']
    },
    {
      ...sharedConfig,
      // A test file belongs to the `integration` project if it touches a real DB
      // (dbManager / cubeDataSource). All others belong to `unit`.
      displayName: 'integration',
      globalSetup: '<rootDir>/test/helpers/global-setup.ts',
      globalTeardown: '<rootDir>/test/helpers/global-teardown.ts',
      // worker-db-env.ts must come first so TEST_DB_DATABASE is set before any app module imports
      setupFiles: ['<rootDir>/test/helpers/worker-db-env.ts', '<rootDir>/test/helpers/jest-setup.ts'],
      roots: ['<rootDir>/test'],
      testMatch: ['<rootDir>/test/integration/**/*.test.ts'],
      maxWorkers: '50%'
    }
  ]
};

export default config;
