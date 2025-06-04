import 'dotenv/config';
import 'reflect-metadata';

import { appConfig } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initDb, initEntitySubscriber } from './db/init';
import { initPassport } from './middleware/passport-auth';
import { getCubeDB } from './db/cube-db';

const PORT = appConfig().backend.port;

Promise.resolve()
  .then(async () => {
    const dbManager = await initDb();
    await initEntitySubscriber(dbManager.getDataSource());

    const cubeDB = getCubeDB();
    const cubeClient = await cubeDB.connect();
    await cubeClient.query('SELECT NOW()');
    cubeClient.release();
    logger.info('Cube DB initialized');

    await initPassport(dbManager.getDataSource());
  })
  .then(() => {
    app.listen(PORT, async () => {
      logger.info(`Server is running on port ${PORT}`);
    });
  })
  .catch((err) => {
    logger.error(err);
    process.exit(1);
  });
