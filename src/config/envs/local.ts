import { Level } from 'pino';

import { AuthProvider } from '../../enums/auth-providers';
import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';

// anything that is not a secret can go in here, get the rest from env

export function getLocalConfig(): AppConfig {
    return defineConfig({
        env: AppEnv.Local,
        frontend: {
            port: parseInt(process.env.FRONTEND_PORT || '3000', 10),
            url: process.env.FRONTEND_URL || 'http://localhost:3000'
        },
        backend: {
            port: parseInt(process.env.BACKEND_PORT || '3001', 10),
            url: process.env.BACKEND_URL || 'http://localhost:3001'
        },
        session: {
            secret: process.env.SESSION_SECRET || 'mysecret',
            secure: false,
            redisUrl: process.env.REDIS_URL || 'redis://localhost'
        },
        logger: {
            level: (process.env.LOG_LEVEL as Level) || 'debug'
        },
        database: {
            host: process.env.DB_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || '5432', 10),
            username: process.env.DB_USERNAME || 'postgres',
            password: process.env.DB_PASSWORD || 'postgres',
            database: process.env.DB_DATABASE || 'statswales-backend',
            ssl: process.env.DB_SSL === 'true' || false,
            synchronize: false
        },
        rateLimit: {
            windowMs: -1 // disable rate limiting on local
        },
        auth: {
            providers: [AuthProvider.Google, AuthProvider.EntraId, AuthProvider.Local],
            jwt: {
                secret: process.env.JWT_SECRET || 'jwtsecret',
                expiresIn: process.env.JWT_EXPIRES_IN || '6h',
                secure: false,
                cookieDomain: 'http://localhost'
            }
        },
        duckdb: {
            threads: process.env.DUCKDB_THREADS ? parseInt(process.env.DUCKDB_THREADS, 10) : 1,
            memory: process.env.DUCKDB_MEMORY || '125MB'
        }
    });
}
