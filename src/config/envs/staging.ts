import { AuthProvider } from '../../enums/auth-providers';
import { AppConfig } from '../app-config.interface';
import { defineConfig } from '../define-config';
import { AppEnv } from '../env.enum';

// anything that is not a secret can go in here, get the rest from env

export function getStagingConfig(): AppConfig {
  return defineConfig({
    env: AppEnv.Staging,
    auth: {
      providers: [AuthProvider.EntraId],
      jwt: {
        cookieDomain: process.env.JWT_COOKIE_DOMAIN || process.env.BACKEND_URL!.replace('api.', '')
      }
    },
    clamav: {
      host: 'clamav',
      port: 3310
    }
  });
}
