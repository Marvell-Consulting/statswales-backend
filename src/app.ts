import 'reflect-metadata';

import express, { Application } from 'express';
import passport from 'passport';
import cookieParser from 'cookie-parser';

import './utils/bigint-patcher';
import { logger, httpLogger } from './utils/logger';
import { appConfig } from './config';
import { checkConfig } from './config/check-config';
import { i18next, i18nextMiddleware } from './middleware/translation';
import { rateLimiter } from './middleware/rate-limiter';
import session from './middleware/session';
import { requestContext } from './middleware/context';
import { authRouter } from './route/auth';
import { healthcheckRouter } from './route/healthcheck';
import { datasetRouter } from './route/dataset';
import { errorHandler } from './route/error-handler';
import { providerRouter } from './route/provider';
import { topicRouter } from './route/topic';
import { organisationRouter } from './route/organisation';
import { teamRouter } from './route/team';
import { translationRouter } from './route/translation';
import { consumerRouter } from './route/consumer';
import { initServices } from './middleware/services';

const app: Application = express();
const config = appConfig();
checkConfig();

logger.info(`App config loaded for '${config.env}' env`);

app.disable('x-powered-by');
app.set('trust proxy', 1);

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(cookieParser());
app.use(session);
app.use(requestContext);
app.use(initServices);

// public routes
app.use('/auth', rateLimiter, authRouter);
app.use('/healthcheck', rateLimiter, healthcheckRouter);
app.use('/published', rateLimiter, consumerRouter);

// authenticated routes
app.use('/dataset', rateLimiter, passport.authenticate('jwt', { session: false }), datasetRouter);
app.use('/provider', rateLimiter, passport.authenticate('jwt', { session: false }), providerRouter);
app.use('/topic', rateLimiter, passport.authenticate('jwt', { session: false }), topicRouter);
app.use('/organisation', rateLimiter, passport.authenticate('jwt', { session: false }), organisationRouter);
app.use('/team', rateLimiter, passport.authenticate('jwt', { session: false }), teamRouter);
app.use('/translation', rateLimiter, passport.authenticate('jwt', { session: false }), translationRouter);

app.use(errorHandler);

export default app;
