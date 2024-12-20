import 'reflect-metadata';

import express, { Application } from 'express';
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
import { errorHandler } from './route/error-handler';
import { providerRouter } from './route/provider';
import { topicRouter } from './route/topic';
import { organisationRouter } from './route/organisation';
import { teamRouter } from './route/team';
import { translationRouter } from './route/translation';

export const initDb = async (): Promise<DatabaseManager> => {
    const dbManager = new DatabaseManager(logger);
    await dbManager.initializeDataSource();
    await initPassport(dbManager.getDataSource().getRepository('User'));
    return dbManager;
};

const app: Application = express();
const config = appConfig();

// DO NOT REMOVE!
// DuckDB handles numbers as bigints.  BigInts don't serialise
// toJSON easily.  This monkypatches BigInt so that if the number
// is less than the max safe interger we return a number otherwise
// we return a string
(BigInt.prototype as any).toJSON = function () {
    if (this < Number.MAX_SAFE_INTEGER) return Number(this);
    return this.toString();
};

logger.info(`App config loaded for '${config.env}' env`);

app.disable('x-powered-by');
app.set('trust proxy', 1);

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(cookieParser());
app.use(session);

app.use('/auth', rateLimiter, authRouter);
app.use('/healthcheck', rateLimiter, healthcheckRouter);
app.use('/dataset', rateLimiter, passport.authenticate('jwt', { session: false }), datasetRouter);
app.use('/provider', rateLimiter, passport.authenticate('jwt', { session: false }), providerRouter);
app.use('/topic', rateLimiter, passport.authenticate('jwt', { session: false }), topicRouter);
app.use('/organisation', rateLimiter, passport.authenticate('jwt', { session: false }), organisationRouter);
app.use('/team', rateLimiter, passport.authenticate('jwt', { session: false }), teamRouter);
app.use('/translation', rateLimiter, passport.authenticate('jwt', { session: false }), translationRouter);

app.use(errorHandler);

export default app;
