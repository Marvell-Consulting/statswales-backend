import 'dotenv/config';
import 'reflect-metadata';

import { appConfig } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initDb, initEntitySubscriber } from './db/init';
import { initPassport } from './middleware/passport-auth';

const PORT = appConfig().backend.port;

Promise.resolve()
    .then(async () => {
        const dbManager = await initDb();
        await initEntitySubscriber(dbManager.getDataSource());
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
