import { Level } from 'pino';

import { AppConfig } from '../app-config.interface';
import { AppEnv } from '../env.enum';
import { SessionStore } from '../session-store.enum';
import { FileStore } from '../file-store.enum';
import { Locale } from '../../enums/locale';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

export const getDefaultConfig = (): AppConfig => {
  return {
    env: AppEnv.Default, // MUST be overridden by other configs
    frontend: {
      port: parseInt(process.env.FRONTEND_PORT || '3000', 10),
      url: process.env.FRONTEND_URL!
    },
    backend: {
      port: parseInt(process.env.BACKEND_PORT || '3000', 10),
      url: process.env.BACKEND_URL!
    },
    language: {
      availableTranslations: [Locale.English, Locale.Welsh],
      supportedLocales: [Locale.EnglishGb, Locale.WelshGb],
      fallback: Locale.English
    },
    session: {
      store: SessionStore.Redis,
      secret: process.env.SESSION_SECRET!,
      secure: true,
      maxAge: parseInt(process.env.SESSION_MAX_AGE || ONE_DAY_MS.toString(), 10),
      redisUrl: process.env.REDIS_URL,
      redisPassword: process.env.REDIS_ACCESS_KEY
    },
    logger: {
      level: (process.env.LOG_LEVEL as Level) || 'info',
      memUsage: process.env.MEM_USAGE ? process.env.MEM_USAGE.toLowerCase() == 'true' : false
    },
    rateLimit: {
      windowMs: 60000,
      maxRequests: 100
    },
    database: {
      host: process.env.DB_HOST!,
      port: parseInt(process.env.DB_PORT || '5432', 10),
      username: process.env.DB_USERNAME!,
      password: process.env.DB_PASSWORD!,
      database: process.env.DB_DATABASE!,
      ssl: true,
      synchronize: false,
      poolSize: parseInt(process.env.DB_POOL_SIZE || '5', 10),
      maxUses: parseInt(process.env.DB_MAX_USES || '7500', 10),
      idleTimeoutMs: parseInt(process.env.DB_IDLE_TIMEOUT_MS || '10000', 10),
      connectionTimeoutMs: parseInt(process.env.DB_CONNECTION_TIMEOUT_MS || '2000', 10)
    },
    auth: {
      providers: [],
      jwt: {
        secret: process.env.JWT_SECRET!,
        expiresIn: process.env.JWT_EXPIRES_IN || '6h',
        secure: true,
        cookieDomain: process.env.JWT_COOKIE_DOMAIN!
      },
      google: {
        clientId: process.env.GOOGLE_CLIENT_ID!,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET!
      },
      entraid: {
        url: process.env.ENTRAID_URL!,
        clientId: process.env.ENTRAID_CLIENT_ID!,
        clientSecret: process.env.ENTRAID_CLIENT_SECRET!
      }
    },
    storage: {
      store: FileStore.DataLake,
      blob: {
        url: process.env.AZURE_BLOB_STORAGE_URL!,
        accountName: process.env.AZURE_BLOB_STORAGE_ACCOUNT_NAME!,
        accountKey: process.env.AZURE_BLOB_STORAGE_ACCOUNT_KEY!,
        containerName: process.env.AZURE_BLOB_STORAGE_CONTAINER_NAME!
      },
      datalake: {
        url:
          process.env.AZURE_DATALAKE_STORAGE_URL ||
          `https://${process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net`,
        accountName: process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_NAME!,
        accountKey: process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_KEY!,
        fileSystemName: process.env.AZURE_DATALAKE_STORAGE_FILESYSTEM_NAME!
      }
    },
    duckdb: {
      threads: process.env.DUCKDB_THREADS ? parseInt(process.env.DUCKDB_THREADS, 10) : 1,
      memory: process.env.DUCKDB_MEMORY || '125MB',
      writeTimeOut: process.env.DUCKDB_WRITE_TIMEOUT ? parseInt(process.env.DUCKDB_WRITE_TIMEOUT, 10) : 150
    },
    clamav: {
      host: process.env.CLAMAV_HOST || 'localhost',
      port: parseInt(process.env.CLAMAV_PORT || '3310', 10),
      timeout: parseInt(process.env.CLAMAV_TIMEOUT || '60000', 10)
    }
  };
};
