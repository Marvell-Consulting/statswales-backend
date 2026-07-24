import { set } from 'lodash';

import { AppEnv } from '../../../src/config/env.enum';
import { AppConfig } from '../../../src/config/app-config.interface';
import { SessionStore } from '../../../src/config/session-store.enum';
import { FileStore } from '../../../src/config/file-store.enum';
import { AuthProvider } from '../../../src/enums/auth-providers';
import { Locale } from '../../../src/enums/locale';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

// a fully populated, production-like config where every required leaf has a real value
const buildValidConfig = (env: AppEnv = AppEnv.Prod): AppConfig => ({
  env,
  build: { gitSha: 'abc123' },
  frontend: { port: 3000, url: 'https://example.com' },
  backend: { port: 3001, url: 'https://api.example.com' },
  healthcheck: { dbTimeoutMs: 2500, storageTimeoutMs: 2500 },
  language: {
    availableTranslations: [Locale.English, Locale.Welsh],
    supportedLocales: [Locale.EnglishGb, Locale.WelshGb],
    fallback: Locale.English
  },
  session: {
    store: SessionStore.Redis,
    secret: 'session-secret',
    secure: true,
    maxAge: 86400000,
    redisUrl: 'redis://localhost',
    redisPassword: 'redis-password'
  },
  logger: { level: 'info', memUsage: false },
  rateLimit: { windowMs: 60000, maxRequests: 100, bypassToken: 'bypass-token' },
  requestTimeout: { defaultMs: 30000, longMs: 300000 },
  database: {
    host: 'db-host',
    port: 5432,
    username: 'db-user',
    password: 'db-password',
    database: 'statswales',
    ssl: true,
    synchronize: false
  },
  auth: {
    providers: [AuthProvider.EntraId],
    jwt: {
      secret: 'jwt-secret',
      expiresIn: '6h',
      secure: true,
      cookieDomain: 'example.com'
    },
    entraid: {
      url: 'https://login.microsoftonline.com/tenant',
      clientId: 'entraid-client-id',
      clientSecret: 'entraid-client-secret'
    }
  },
  storage: {
    store: FileStore.DataLake,
    blob: {
      url: 'https://account.blob.core.windows.net',
      accountName: 'account',
      accountKey: 'blob-account-key',
      containerName: 'container'
    },
    datalake: {
      url: 'https://account.dfs.core.windows.net',
      accountName: 'account',
      accountKey: 'datalake-account-key',
      fileSystemName: 'filesystem'
    }
  },
  duckdb: { threads: 1, memory: '256MB', writeTimeOut: 150, maxConcurrency: 5 },
  clamav: { host: 'clamav', port: 3310, timeout: 60000 },
  cube_builder: { preserve_failed: false }
});

// checkConfig reads `config` from the module scope of `src/config` at call time, so each test loads a
// fresh, isolated copy of check-config.ts mocked against its own config object rather than mutating a
// shared singleton
const checkConfigFor = (config: AppConfig): (() => void) => {
  let checkConfig!: () => void;

  jest.isolateModules(() => {
    jest.doMock('../../../src/config', () => ({ config }));
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    checkConfig = require('../../../src/config/check-config').checkConfig;
  });

  return checkConfig;
};

describe('checkConfig', () => {
  it('does not throw for a fully populated production-like config', () => {
    expect(() => checkConfigFor(buildValidConfig(AppEnv.Prod))()).not.toThrow();
  });

  describe.each([
    ['storage.blob.accountKey', 'AZURE_BLOB_STORAGE_ACCOUNT_KEY'],
    ['storage.datalake.accountKey', 'AZURE_DATALAKE_STORAGE_ACCOUNT_KEY'],
    ['auth.entraid.clientSecret', 'ENTRAID_CLIENT_SECRET'],
    ['session.secret', 'SESSION_SECRET']
  ])('%s (%s)', (path) => {
    it('fails boot in a prod-like config when the value is missing', () => {
      const config = buildValidConfig(AppEnv.Prod);
      set(config, path, undefined);

      expect(() => checkConfigFor(config)()).toThrow(`${path} is invalid or missing`);
    });

    it('fails boot in a prod-like config when the value is an empty string', () => {
      const config = buildValidConfig(AppEnv.Prod);
      set(config, path, '');

      expect(() => checkConfigFor(config)()).toThrow(`${path} is invalid or missing`);
    });

    it('fails boot in a prod-like config when the value is a blank/whitespace-only string', () => {
      const config = buildValidConfig(AppEnv.Prod);
      set(config, path, '   ');

      expect(() => checkConfigFor(config)()).toThrow(`${path} is invalid or missing`);
    });
  });

  describe('local/CI leniency for unconfigured auth providers and storage backends', () => {
    // buildValidConfig defaults to storage.store: FileStore.DataLake and auth.providers: [AuthProvider.EntraId],
    // so blob is the *unused* storage backend here - only its secret can be safely missing.
    it('does not throw when the unused storage backend secret is missing in a local config', () => {
      const config = buildValidConfig(AppEnv.Local);
      set(config, 'storage.blob.accountKey', undefined);

      expect(() => checkConfigFor(config)()).not.toThrow();
    });

    it('does not throw when the unused storage backend secret is missing in a CI config', () => {
      const config = buildValidConfig(AppEnv.Ci);
      set(config, 'storage.blob.accountKey', undefined);

      expect(() => checkConfigFor(config)()).not.toThrow();
    });

    it('does not throw when entraid secrets are missing and EntraId is not an enabled auth provider', () => {
      const config = buildValidConfig(AppEnv.Local);
      config.auth.providers = [AuthProvider.Jwt];
      set(config, 'auth.entraid.clientSecret', undefined);

      expect(() => checkConfigFor(config)()).not.toThrow();
    });

    it('still fails boot in a local config when the *selected* storage backend is missing its secret', () => {
      // storage.store is FileStore.DataLake by default, so datalake credentials are in use here
      const config = buildValidConfig(AppEnv.Local);
      set(config, 'storage.datalake.accountKey', undefined);

      expect(() => checkConfigFor(config)()).toThrow('storage.datalake.accountKey is invalid or missing');
    });

    it('still fails boot in a CI config when the *selected* storage backend is missing its secret', () => {
      const config = buildValidConfig(AppEnv.Ci);
      set(config, 'storage.datalake.accountKey', undefined);

      expect(() => checkConfigFor(config)()).toThrow('storage.datalake.accountKey is invalid or missing');
    });

    it('still fails boot in a local config when storage.store is an unrecognised value, since getFileService() falls back to datalake for anything that is not exactly "blob"', () => {
      const config = buildValidConfig(AppEnv.Local);
      // simulates an unvalidated/garbage FILE_STORE env var - local.ts casts it with `as FileStore`
      // without validating it against the enum, so this reaches checkConfig as a non-empty string
      config.storage.store = 'not-a-real-store' as FileStore;
      set(config, 'storage.datalake.accountKey', undefined);

      expect(() => checkConfigFor(config)()).toThrow('storage.datalake.accountKey is invalid or missing');
    });

    it('still fails boot in a local config when EntraId is an enabled auth provider but its secret is missing', () => {
      // auth.providers includes AuthProvider.EntraId by default, so entraid credentials are in use here
      const config = buildValidConfig(AppEnv.Local);
      set(config, 'auth.entraid.clientSecret', undefined);

      expect(() => checkConfigFor(config)()).toThrow('auth.entraid.clientSecret is invalid or missing');
    });

    it('still fails boot in a local config when session.secret is empty', () => {
      const config = buildValidConfig(AppEnv.Local);
      set(config, 'session.secret', '');

      expect(() => checkConfigFor(config)()).toThrow('session.secret is invalid or missing');
    });
  });

  it('does not throw for genuinely optional properties when missing', () => {
    const config = buildValidConfig(AppEnv.Prod);
    set(config, 'session.redisUrl', undefined);
    set(config, 'session.redisPassword', undefined);
    set(config, 'rateLimit.bypassToken', undefined);

    expect(() => checkConfigFor(config)()).not.toThrow();
  });
});
