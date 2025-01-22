import 'dotenv/config';
import 'reflect-metadata';

import { appConfig } from './config';
import app from './app';
import { logger } from './utils/logger';
import { initDb, initEntitySubscriber } from './db/init';
import { initPassport } from './middleware/passport-auth';

const PORT = appConfig().backend.port;

app.listen(PORT, async () => {
    const dbManager = await initDb();
    await initEntitySubscriber(dbManager.getDataSource());
    await initPassport(dbManager.getDataSource());
    logger.info(`Server is running on port ${PORT}`);
});
