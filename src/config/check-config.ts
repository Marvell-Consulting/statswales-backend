import { logger } from '../utils/logger';
import { walkObject, UnknownObject } from '../utils/walk-object';

import { optionalProperties } from './app-config.interface';

import { config } from '.';

export const checkConfig = (): void => {
  logger.info('Checking app config...');

  walkObject(config as unknown as UnknownObject, ({ value, location, isLeaf }) => {
    const configPath = location.join('.');
    const optional = optionalProperties.some((prop) => configPath.includes(prop));

    if (isLeaf && !optional && value === undefined) {
      throw new Error(`${configPath} is invalid or missing, stopping server`);
    }
  });

  logger.info(`App config loaded for '${config.env}' env`);
};
