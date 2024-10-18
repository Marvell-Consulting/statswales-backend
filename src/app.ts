import 'reflect-metadata';

import express, { Application, Request, Response } from 'express';
import passport from 'passport';
import cookieParser from 'cookie-parser';

import { logger, httpLogger } from './utils/logger';
import { appConfig } from './config';
import DatabaseManager from './db/database-manager';
import { i18next, i18nextMiddleware } from './middleware/translation';
import { initPassport } from './middleware/passport-auth';
import { rateLimiter } from './middleware/rate-limiter';
import session from './middleware/session';
import { authRouter } from './route/auth';
import { healthcheckRouter } from './route/healthcheck';
import { datasetRouter } from './route/dataset';

export const initDb = async (): Promise<DatabaseManager> => {
    const dbManager = new DatabaseManager(logger);
    await dbManager.initializeDataSource();
    await initPassport(dbManager.getDataSource().getRepository('User'));
    return dbManager;
};

const app: Application = express();
const config = appConfig();

logger.info(`App config loaded for '${config.env}' env`);

app.disable('x-powered-by');
app.set('trust proxy', 1);

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(cookieParser());
app.use(express.json());
app.use(session);

app.use('/auth', rateLimiter, authRouter);
app.use('/healthcheck', rateLimiter, healthcheckRouter);
app.use('/dataset', rateLimiter, passport.authenticate('jwt', { session: false }), datasetRouter);

export default app;
