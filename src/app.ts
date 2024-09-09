import 'reflect-metadata';

import express, { Application, Request, Response } from 'express';
import passport from 'passport';

import { logger, httpLogger } from './utils/logger';
import { dataSource } from './data-source';
import DatabaseManager from './database-manager';
import { i18next, i18nextMiddleware } from './middleware/translation';
import { initPassport } from './middleware/passport-auth';
import { rateLimiter } from './middleware/rate-limiter';
import { apiRoute as datasetRoutes } from './route/dataset-route';
import { healthcheck as healthCheckRoutes } from './route/healthcheck';
import { test as testRoutes } from './route/test';
import { auth as authRoutes } from './route/auth';
import session from './middleware/session';

// eslint-disable-next-line import/no-mutable-exports
export let dbManager: DatabaseManager;

const connectToDb = async () => {
    dbManager = new DatabaseManager(dataSource, logger);
    await dbManager.initializeDataSource();
    await initPassport(dbManager.getDataSource().getRepository('User'));
};

connectToDb();

const app: Application = express();

app.disable('x-powered-by');

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(express.json());
app.use(session);

app.use('/auth', rateLimiter, authRoutes);
app.use('/test', rateLimiter, testRoutes);
app.use('/healthcheck', rateLimiter, healthCheckRoutes);
app.use('/:lang/dataset', rateLimiter, passport.authenticate('jwt'), datasetRoutes);
app.use('/:lang/healthcheck', rateLimiter, healthCheckRoutes);

app.get('/', (req: Request, res: Response) => {
    const lang = req.headers['accept-language'] || req.headers['Accept-Language'] || req.i18n.language || 'en-GB';
    if (lang.includes('cy')) {
        res.redirect('/cy-GB/api');
    } else {
        res.redirect('/en-GB/api');
    }
});

export default app;
