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
  build: {
    gitSha: string;
  };
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
    bypassToken?: string;
  };
  requestTimeout: {
    defaultMs: number;
    longMs: number;
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
    consumerPoolSize?: number;
    publisherPoolSize?: number;
    maxUses?: number;
    idleTimeoutMs?: number;
    connectionTimeoutMs?: number;
    appStatementTimeoutMs?: number;
    cubeStatementTimeoutMs?: number;
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
    maxConcurrency: number;
  };
  clamav: {
    host: string;
    port: number;
    timeout: number;
  };
  cube_builder: {
    preserve_failed: boolean;
  };
}

// list any optional leaf properties here so we can ignore missing values when we check the config on boot.
// matched against the final segment of the config path (eg. 'session.redisUrl' -> 'redisUrl'), never as a
// substring, so this must only ever contain full property names.
// it would be nice to get them directly from the interface, but interfaces are compile-time only
export const optionalProperties = ['redisUrl', 'redisPassword', 'bypassToken'];

// config blocks that hold real secrets/credentials which are only exercised once the corresponding auth
// provider or storage backend is actually used. These can legitimately be left unset for local development
// and CI (where that provider/backend isn't configured), but must always be present once deployed
// (staging/production) - see checkConfig in check-config.ts.
export const devOptionalBlocks = ['entraid', 'blob', 'datalake'];
