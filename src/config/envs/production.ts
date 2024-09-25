import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';

// anything that is not a secret can go in here, get the rest from env

export function getProductionConfig(): AppConfig {
    return defineConfig({
        env: AppEnv.PROD
    });
}
