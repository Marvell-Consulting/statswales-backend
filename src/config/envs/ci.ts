import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';
import { SessionStore } from '../session-store.enum';

// anything that is not a secret can go in here, get the rest from env

export function getCIConfig(): AppConfig {
    return defineConfig({
        env: AppEnv.Ci,
        logger: {
            level: 'error'
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
            synchronize: true
        },
        auth: {
            providers: ['local'],
            jwt: {
                secret: process.env.JWT_SECRET || 'jwtsecret',
                expiresIn: process.env.JWT_EXPIRES_IN || '6h',
                cookieDomain: 'http://localhost'
            }
        },
        storage: {
            blob: {
                accountName: 'accountname',
                accountKey: 'accountkey',
                containerName: 'containername'
            },
            datalake: {
                accountName: 'accountname',
                accountKey: 'accountkey',
                fileSystemName: 'fsname',
                directoryName: 'dirname'
            }
        }
    });
}
