import { AuthProvider } from '../../enums/auth-providers';
import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';

// anything that is not a secret can go in here, get the rest from env

export function getProductionConfig(): AppConfig {
  return defineConfig({
    env: AppEnv.Prod,
    auth: {
      providers: [AuthProvider.EntraId],
      jwt: {
        cookieDomain: process.env.BACKEND_URL!.replace('api.', '')
      }
    }
  });
}
