/* eslint-disable import/no-cycle */
import 'reflect-metadata';

import pino, { Logger } from 'pino';
import express, { Application, Request, Response } from 'express';
import i18next from 'i18next';
import Backend from 'i18next-fs-backend';
import i18nextMiddleware from 'i18next-http-middleware';
import { DataSourceOptions } from 'typeorm';

import { apiRoute } from './route/dataset-route';
import { healthcheck } from './route/healthcheck';
import DatabaseManager from './database-manager';

// eslint-disable-next-line import/no-mutable-exports
export let dbManager: DatabaseManager;

export const logger: Logger = pino({
    name: 'StatsWales-Alpha-App',
    level: 'debug'
});

export const connectToDb = async (datasourceOptions: DataSourceOptions) => {
    dbManager = new DatabaseManager(datasourceOptions, logger);
    await dbManager.initializeDataSource();
};

i18next
    .use(Backend)
    .use(i18nextMiddleware.LanguageDetector)
    .init({
        detection: {
            order: ['path', 'header'],
            lookupHeader: 'accept-language',
            caches: false,
            ignoreRoutes: ['/healthcheck', '/public', '/css', '/assets']
        },
        backend: {
            loadPath: `${__dirname}/resources/locales/{{lng}}.json`
        },
        fallbackLng: 'en-GB',
        preload: ['en-GB', 'cy-GB'],
        debug: false
    });

const app: Application = express();

app.use(i18nextMiddleware.handle(i18next));
app.use('/:lang/dataset', apiRoute);
app.use('/:lang/healthcheck', healthcheck);
app.use('/healthcheck', healthcheck);

app.get('/', (req: Request, res: Response) => {
    const lang = req.headers['accept-language'] || req.headers['Accept-Language'] || req.i18n.language || 'en-GB';
    if (lang.includes('cy')) {
        res.redirect('/cy-GB/api');
    } else {
        res.redirect('/en-GB/api');
    }
});

export default app;
