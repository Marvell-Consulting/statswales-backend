import 'dotenv/config';
import 'reflect-metadata';

import { appConfig } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initPassport } from './middleware/passport-auth';
import fs from 'node:fs';
import { multerStorageDir } from './config/multer-storage';
import { dbManager } from './db/database-manager';

const PORT = appConfig().backend.port;

Promise.resolve()
  .then(async () => {
    await dbManager.initDataSources();
    await dbManager.initEntitySubscriber();
    await initPassport(dbManager.getAppDataSource());
  })
  .then(() => {
    if (!fs.existsSync(multerStorageDir)) {
      fs.mkdirSync(multerStorageDir);
    }
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
