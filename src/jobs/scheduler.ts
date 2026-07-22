import { schedule } from 'node-cron';

import { logger } from '../utils/logger';
import { config } from '../config';
import { runNightlyCleanup } from '../services/cleanup';

export function startScheduledJobs(): void {
  if (!config.cron.enabled) {
    logger.info('cron: scheduled jobs disabled');
    return;
  }

  const { nightlyCleanupSchedule, timezone } = config.cron;

  const task = schedule(
    nightlyCleanupSchedule,
    () => runNightlyCleanup(config.cleanup.staleBuildTimeoutMs, config.cleanup.staleTempFileTimeoutMs),
    { name: 'nightly-cleanup', timezone, noOverlap: true }
  );

  task.on('task:failed', ({ error }) => {
    logger.error(error, 'cron: nightly cleanup job threw unexpectedly');
  });

  logger.info(`cron: nightly cleanup scheduled ("${nightlyCleanupSchedule}", ${timezone})`);
}
