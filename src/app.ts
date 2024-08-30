/* eslint-disable import/no-cycle */
import 'reflect-metadata';

import express, { Application, Request, Response } from 'express';
import passport from 'passport';

import { logger, httpLogger } from './utils/logger';
import { dataSource } from './data-source';
import DatabaseManager from './database-manager';
import { i18next, i18nextMiddleware } from './middleware/translation';
import { initPassport } from './middleware/passport-auth';
import { apiRoute as datasetRoutes } from './route/dataset-route';
import { healthcheck as healthCheckRoutes } from './route/healthcheck';
import { auth as authRoutes } from './route/auth';

// eslint-disable-next-line import/no-mutable-exports
export let dbManager: DatabaseManager;

const connectToDb = async () => {
    dbManager = new DatabaseManager(dataSource, logger);
    await dbManager.initializeDataSource();
    initPassport(dbManager.getDataSource().getRepository('User'));
};

connectToDb();

const app: Application = express();

app.disable('x-powered-by');

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(express.json());

app.use('/auth', authRoutes);
app.use('/healthcheck', healthCheckRoutes);
app.use('/:lang/dataset', passport.authenticate('jwt'), datasetRoutes);
app.use('/:lang/healthcheck', healthCheckRoutes);

app.get('/', (req: Request, res: Response) => {
    const lang = req.headers['accept-language'] || req.headers['Accept-Language'] || req.i18n.language || 'en-GB';
    if (lang.includes('cy')) {
        res.redirect('/cy-GB/api');
    } else {
        res.redirect('/en-GB/api');
    }
});

export default app;
