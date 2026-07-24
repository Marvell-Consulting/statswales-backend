import { logger } from '../utils/logger';
import { walkObject, UnknownObject } from '../utils/walk-object';

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
      (devLikeEnv && devOptionalBlocks.some((block) => location.includes(block)));

    if (isLeaf && !optional && isMissing(value)) {
      throw new Error(`${configPath} is invalid or missing, stopping server`);
    }
  });

  logger.info(`App config loaded for '${config.env}' env`);
};
