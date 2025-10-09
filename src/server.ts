import 'dotenv/config';
import 'reflect-metadata';

import { config } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initPassport } from './middleware/passport-auth';
import { dbManager } from './db/database-manager';

const PORT = config.backend.port;

Promise.resolve()
  .then(async () => {
    await dbManager.initDataSources();
    await dbManager.initEntitySubscriber();
    await initPassport(dbManager.getAppDataSource());
  })
  .then(() => {
    app.listen(PORT, async () => {
      logger.info(`Server is running on port ${PORT}`);
    });
  })
  .catch(async (err) => {
    logger.error(err);
    await dbManager.destroyDataSources();
    process.exit(1);
  });
