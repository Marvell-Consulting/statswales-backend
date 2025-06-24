import { logger } from '../utils/logger';
import { walkObject, UnknownObject } from '../utils/walk-object';

import { optionalProperties } from './app-config.interface';

import { appConfig } from '.';

export const checkConfig = (): void => {
  const config = appConfig() as unknown as UnknownObject;

  logger.info('Checking app config...');

  walkObject(config, ({ value, location, isLeaf }) => {
    const configPath = location.join('.');
    const optional = optionalProperties.some((prop) => configPath.includes(prop));

    if (isLeaf && !optional && value === undefined) {
      throw new Error(`${configPath} is invalid or missing, stopping server`);
    }
  });
};
