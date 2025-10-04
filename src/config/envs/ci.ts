import { Level } from 'pino';

import { AuthProvider } from '../../enums/auth-providers';
import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';
import { SessionStore } from '../session-store.enum';
import { FileStore } from '../file-store.enum';

// anything that is not a secret can go in here, get the rest from env

export function getCIConfig(): AppConfig {
  return defineConfig({
    env: AppEnv.Ci,
    logger: {
      level: (process.env.LOG_LEVEL as Level) || 'debug'
    },
    frontend: {
      port: parseInt(process.env.FRONTEND_PORT || '3000', 10),
      url: process.env.FRONTEND_URL || 'http://localhost:3000'
    },
    backend: {
      port: parseInt(process.env.BACKEND_PORT || '3001', 10),
      url: process.env.BACKEND_URL || 'http://localhost:3001'
    },
    session: {
      store: SessionStore.Memory,
      secret: process.env.SESSION_SECRET || 'mysecret',
      secure: false
    },
    database: {
      host: process.env.TEST_DB_HOST || 'localhost',
      port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
      username: process.env.TEST_DB_USERNAME || 'postgres',
      password: process.env.TEST_DB_PASSWORD || 'postgres',
      database: process.env.TEST_DB_DATABASE || 'statswales-backend-test',
      ssl: false,
      synchronize: process.env.TEST_DB_SYNC === 'true' ? true : false
    },
    rateLimit: {
      windowMs: -1 // disable rate limiting in CI
    },
    auth: {
      providers: [AuthProvider.Local],
      jwt: {
        secret: process.env.JWT_SECRET || 'jwtsecret',
        expiresIn: process.env.JWT_EXPIRES_IN || '6h',
        cookieDomain: 'http://localhost'
      }
    },
    storage: {
      store: FileStore.Blob,
      blob: {
        // defaults to dev credentials provided in the Azurite docs
        // @see https://github.com/Azure/Azurite?tab=readme-ov-file#default-storage-account
        url: process.env.AZURE_BLOB_STORAGE_URL || 'http://127.0.0.1:10000',
        accountName: process.env.AZURE_BLOB_STORAGE_ACCOUNT_NAME || 'devstoreaccount1',
        accountKey:
          process.env.AZURE_BLOB_STORAGE_ACCOUNT_KEY ||
          'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
        containerName: process.env.AZURE_BLOB_STORAGE_CONTAINER_NAME || 'sw3test'
      }
    },
    duckdb: {
      threads: 2,
      memory: '256MB'
    }
  });
}
