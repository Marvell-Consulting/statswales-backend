import { getLocalConfig } from './envs/local';
import { getCIConfig } from './envs/ci';
import { getStagingConfig } from './envs/staging';
import { getProductionConfig } from './envs/production';
import { AppEnv } from './env.enum';
import { AppConfig } from './app-config.interface';

// this is loosely based on the config strategy from
// https://www.raulmelo.me/en/blog/best-practices-for-handling-per-environment-config-js-ts-applications

export const appConfig = (): AppConfig => {
    const currentEnv = process.env.APP_ENV as AppEnv;

    switch (currentEnv) {
        case AppEnv.LOCAL:
            return getLocalConfig();

        case AppEnv.CI:
            return getCIConfig();

        case AppEnv.STAGING:
            return getStagingConfig();

        case AppEnv.PROD:
            return getProductionConfig();

        case AppEnv.DEFAULT:
        default:
            throw new Error(`Invalid APP_ENV "${currentEnv}"`);
    }
};
