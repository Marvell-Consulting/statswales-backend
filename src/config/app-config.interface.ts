import { Level } from 'pino';

import { Locale } from '../enums/locale';
import { AuthProvider } from '../enums/auth-providers';

import { AppEnv } from './env.enum';
import { SessionStore } from './session-store.enum';

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
    };
    auth: {
        providers: AuthProvider[];
        jwt: {
            secret: string;
            expiresIn: string;
            secure: boolean;
            cookieDomain: string;
        };
        google: {
            clientId: string;
            clientSecret: string;
        };
        entraid: {
            url: string;
            clientId: string;
            clientSecret: string;
        };
    };
    storage: {
        blob: {
            accountName: string;
            accountKey: string;
            containerName: string;
        };
        datalake: {
            accountName: string;
            accountKey: string;
            fileSystemName: string;
            directoryName: string;
        };
    };
}

// list any optional properties here so we can ignore missing values when we check the config on boot
// it would be nice to get them directly from the interface, but interfaces are compile-time only
export const optionalProperties = ['redisUrl', 'redisPassword', 'google', 'entraid', 'blob', 'datalake'];
