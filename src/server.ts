import 'dotenv/config';
import 'reflect-metadata';

import { appConfig } from './config';
import app, { initDb } from './app';
import { logger } from './utils/logger';

const PORT = appConfig().backend.port;

initDb();

app.listen(PORT, () => {
    logger.info(`Server is running on port ${PORT}`);
});
