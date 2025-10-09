import { getLocalConfig } from './envs/local';
import { getCIConfig } from './envs/ci';
import { getStagingConfig } from './envs/staging';
import { getProductionConfig } from './envs/production';
import { AppEnv } from './env.enum';
import { AppConfig } from './app-config.interface';

// this is loosely based on the config strategy from
// https://www.raulmelo.me/en/blog/best-practices-for-handling-per-environment-config-js-ts-applications

export const getConfig = (): AppConfig => {
  const currentEnv = process.env.APP_ENV as AppEnv;

  switch (currentEnv) {
    case AppEnv.Local:
      return getLocalConfig();

    case AppEnv.Ci:
      return getCIConfig();

    case AppEnv.Staging:
      return getStagingConfig();

    case AppEnv.Prod:
      return getProductionConfig();

    case AppEnv.Default:
    default:
      throw new Error(`Invalid APP_ENV "${currentEnv}"`);
  }
};

export const config = getConfig();
