import 'dotenv/config';
import 'reflect-metadata';

import { logger } from '../utils/logger';
import { dbManager } from '../db/database-manager';
import { cleanupSupersededMaterializedViews } from '../services/cleanup';

// Standalone entry point for the "clean up old cubes" job (SW-1309), run on a schedule as its own
// Azure container app job rather than an in-process cron task - see the terraform job definition
// for the schedule. Kept separate from server.ts so this can be triggered, monitored and scaled
// independently of the web process.
Promise.resolve()
  .then(async () => {
    await dbManager.initDataSources();
    logger.info('cleanup-old-cubes: starting');
    await cleanupSupersededMaterializedViews();
    logger.info('cleanup-old-cubes: complete');
  })
  .catch((err) => {
    logger.error(err, 'cleanup-old-cubes: job failed');
    process.exitCode = 1;
  })
  .finally(async () => {
    await dbManager.destroyDataSources();
  });
