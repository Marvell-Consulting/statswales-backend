import { logger } from './logger';
import { appConfig } from '../config';

const config = appConfig();

export const performanceReporting = (totalTime: number, targetNumber: number, method: string) => {
  if (totalTime > targetNumber) {
    logger.warn(`${method} took ${targetNumber} ms`);
  } else {
    logger.debug(`${method} took ${targetNumber} ms`);
  }
  if (config.logger.memUsage) {
    for (const [key, value] of Object.entries(process.memoryUsage())) {
      logger.debug(`Memory usage by ${key}, ${value / 1000000}MB `);
    }
  }
};
