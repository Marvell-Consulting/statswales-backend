import { AppConfig } from '../app-config.interface';
import { AppEnv } from '../env.enum';
import { SessionStore } from '../session-store.enum';

const ONE_DAY = 24 * 60 * 60 * 1000;

export const getDefaultConfig = (): AppConfig => {
    return {
        env: AppEnv.DEFAULT, // MUST be overridden by other configs
        frontend: {
            port: parseInt(process.env.FRONTEND_PORT!, 10),
            url: process.env.FRONTEND_URL!
        },
        backend: {
            port: parseInt(process.env.BACKEND_PORT!, 10),
            url: process.env.BACKEND_URL!
        },
        session: {
            store: process.env.SESSION_STORE! as SessionStore,
            secret: process.env.SESSION_SECRET!,
            secure: true,
            maxAge: parseInt(process.env.SESSION_MAX_AGE || ONE_DAY.toString(), 10),
            redisUrl: process.env.REDIS_URL,
            redisPassword: process.env.REDIS_ACCESS_KEY
        },
        logger: {
            level: process.env.LOGGER_LEVEL || 'info'
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
            synchronize: false
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
            oneLogin: {
                url: process.env.ONELOGIN_URL!,
                clientId: process.env.ONELOGIN_CLIENT_ID!,
                clientSecret: process.env.ONELOGIN_CLIENT_SECRET!,
                publicKey: process.env.ONELOGIN_PUBLIC_KEY!,
                privateKey: process.env.ONELOGIN_PRIVATE_KEY!
            }
        },
        storage: {
            blob: {
                accountName: process.env.AZURE_BLOB_STORAGE_ACCOUNT_NAME!,
                accountKey: process.env.AZURE_BLOB_STORAGE_ACCOUNT_KEY!,
                containerName: process.env.AZURE_BLOB_STORAGE_CONTAINER_NAME!
            },
            datalake: {
                accountName: process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_NAME!,
                accountKey: process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_KEY!,
                fileSystemName: process.env.AZURE_DATALAKE_STORAGE_FILESYSTEM_NAME!,
                directoryName: process.env.AZURE_DATALAKE_STORAGE_DIRECTORY_NAME!
            }
        }
    };
};
