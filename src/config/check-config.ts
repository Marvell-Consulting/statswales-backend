import { logger } from '../utils/logger';
import { walkObject, UnknownObject } from '../utils/walk-object';
import { FileStore } from './file-store.enum';
import { AuthProvider } from '../enums/auth-providers';

import { optionalProperties, devOptionalBlocks } from './app-config.interface';
import { AppEnv } from './env.enum';

import { config } from '.';

// local dev and CI don't necessarily have every auth provider/storage backend configured
// (eg. Entra ID or Azure storage credentials), so devOptionalBlocks are only enforced once deployed
const DEV_LIKE_ENVS: AppEnv[] = [AppEnv.Local, AppEnv.Ci];

// treat blank/whitespace-only strings the same as a missing value, otherwise an empty env var
// (eg. `SESSION_SECRET=`) would silently satisfy the check
const isMissing = (value: unknown): boolean =>
  value === undefined || (typeof value === 'string' && value.trim() === '');

// A devOptionalBlock is only actually optional in local/CI while it's unused. If the app is configured
// to use that specific auth provider or storage backend (eg. storage.store === 'datalake'), its
// credentials are required even in dev-like envs - otherwise checkConfig would pass locally/in CI and
// only fail later, at first request, since getFileService() (called on every request via initServices)
// eagerly constructs the selected storage backend.
//
// getFileService() picks the backend with `config.storage.store === FileStore.Blob ? Blob : DataLake` -
// DataLake is its fallback for *any* non-Blob value, not just the literal 'datalake' - so 'datalake' is
// mirrored here as "not Blob" rather than "exactly DataLake". Matching that fallback exactly means an
// unvalidated/garbage FILE_STORE value (local.ts casts it with `as FileStore` and doesn't validate it
// against the enum) still correctly requires datalake credentials, since that's what would actually run.
const isBlockInUse = (block: string): boolean => {
  switch (block) {
    case 'blob':
      return config.storage.store === FileStore.Blob;
    case 'datalake':
      return config.storage.store !== FileStore.Blob;
    case 'entraid':
      return config.auth.providers.includes(AuthProvider.EntraId);
    default:
      return false;
  }
};

export const checkConfig = (): void => {
  logger.info('Checking app config...');

  const devLikeEnv = DEV_LIKE_ENVS.includes(config.env);

  walkObject(config as unknown as UnknownObject, ({ value, location, isLeaf }) => {
    const configPath = location.join('.');
    const leafName = location[location.length - 1];

    // match on exact path segments only - never as a substring of configPath - so eg. 'blob' can't
    // accidentally match an unrelated property that merely contains 'blob' somewhere in its name
    const optional =
      optionalProperties.includes(String(leafName)) ||
      (devLikeEnv && devOptionalBlocks.some((block) => location.includes(block) && !isBlockInUse(block)));

    if (isLeaf && !optional && isMissing(value)) {
      throw new Error(`${configPath} is invalid or missing, stopping server`);
    }
  });

  logger.info(`App config loaded for '${config.env}' env`);
};
