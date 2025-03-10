import { merge } from 'lodash';
import { DeepPartial } from 'typeorm';

import { AppConfig } from './app-config.interface';
import { getDefaultConfig } from './envs/default';

export function defineConfig(config: DeepPartial<AppConfig>): AppConfig {
  return merge({}, getDefaultConfig(), config);
}
