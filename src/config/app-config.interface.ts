import { Level } from 'pino';

import { Locale } from '../enums/locale';
import { AuthProvider } from '../enums/auth-providers';

import { AppEnv } from './env.enum';
import { SessionStore } from './session-store.enum';
import { FileStore } from './file-store.enum';

export interface JWTConfig {
  secret: string;
  expiresIn: string;
  secure: boolean;
  cookieDomain: string;
}

export interface EntraIdConfig {
  url: string;
  clientId: string;
  clientSecret: string;
}

export interface AppConfig {
  env: AppEnv;
  frontend: {
    port: number;
    url: string;
  };
  backend: {
    port: number;
    url: string;
  };
  healthcheck: {
    dbTimeoutMs: number;
    storageTimeoutMs: number;
  };
  language: {
    availableTranslations: Locale[];
    supportedLocales: Locale[];
    fallback: Locale;
  };
  session: {
    store: SessionStore;
    secret: string;
    secure: boolean;
    maxAge: number;
    redisUrl?: string;
    redisPassword?: string;
  };
  logger: {
    level: Level | 'silent';
    memUsage: boolean;
  };
  rateLimit: {
    windowMs: number;
    maxRequests: number;
  };
  database: {
    host: string;
    port: number;
    username: string;
    password: string;
    database: string;
    ssl?: boolean;
    synchronize?: boolean;
    poolSize?: number;
    maxUses?: number;
    idleTimeoutMs?: number;
    connectionTimeoutMs?: number;
  };
  auth: {
    providers: AuthProvider[];
    jwt: JWTConfig;
    entraid: EntraIdConfig;
  };
  storage: {
    store: FileStore;
    blob: {
      url: string;
      accountName: string;
      accountKey: string;
      containerName: string;
    };
    datalake: {
      url: string;
      accountName: string;
      accountKey: string;
      fileSystemName: string;
    };
  };
  duckdb: {
    threads: number;
    memory: string;
    writeTimeOut: number;
  };
  clamav: {
    host: string;
    port: number;
    timeout: number;
  };
}

// list any optional properties here so we can ignore missing values when we check the config on boot
// it would be nice to get them directly from the interface, but interfaces are compile-time only
export const optionalProperties = ['redisUrl', 'redisPassword', 'entraid', 'blob', 'datalake'];
