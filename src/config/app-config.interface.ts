import { AppEnv } from './env.enum';

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
    session: {
        secret: string;
        secure: boolean;
    };
    logger: {
        level: string;
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
        providers: string[];
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
        oneLogin: {
            url: string;
            clientId: string;
            clientSecret: string;
            publicKey: string;
            privateKey: string;
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
