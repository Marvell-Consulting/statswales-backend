import 'reflect-metadata';

import express, { Application } from 'express';
import passport from 'passport';
import cookieParser from 'cookie-parser';

import './utils/bigint-patcher';
import { httpLogger } from './utils/logger';
import { checkConfig } from './config/check-config';
import { i18next, i18nextMiddleware } from './middleware/translation';
import { rateLimiter } from './middleware/rate-limiter';
import session from './middleware/session';
import { requestContext } from './middleware/context';
import { authRouter } from './routes/auth';
import { healthcheckRouter } from './routes/healthcheck';
import { datasetRouter } from './routes/dataset';
import { errorHandler } from './routes/error-handler';
import { providerRouter } from './routes/provider';
import { topicRouter } from './routes/topic';
import { organisationRouter } from './routes/organisation';
import { translationRouter } from './routes/translation';
import { initServices } from './middleware/services';
import { adminRouter } from './routes/admin';
import { devRouter } from './routes/developer';
import { taskRouter } from './routes/task';
import { userRouter } from './routes/user';
import { publicApiRouter } from './routes/consumer/v1/api';
import { apiDocRouter } from './routes/consumer/v1/docs';
import { publicApiV2Router } from './routes/consumer/v2/api';
// import { apiV2DocRouter } from './routes/consumer/v2/docs';
import { strictTransport } from './middleware/strict-transport';
import { buildLogRouter } from './routes/build-log';

const app: Application = express();
checkConfig();

app.disable('x-powered-by');
app.set('trust proxy', 1);

app.use(httpLogger);
app.use(i18nextMiddleware.handle(i18next));
app.use(cookieParser());
app.use(session);
app.use(requestContext);
app.use(strictTransport);
app.use(initServices);

// public routes
app.use('/auth', rateLimiter, authRouter);
app.use('/healthcheck', rateLimiter, healthcheckRouter);
app.use('/v1/docs', rateLimiter, apiDocRouter);
app.use('/v1', rateLimiter, publicApiRouter);
// app.use('/v2/docs', rateLimiter, apiV2DocRouter);
app.use('/v2', rateLimiter, publicApiV2Router);

const jwtAuth = passport.authenticate('jwt', { session: false });

// authenticated routes
app.use('/dataset', rateLimiter, jwtAuth, datasetRouter);
app.use('/build', rateLimiter, jwtAuth, buildLogRouter);
app.use('/provider', rateLimiter, jwtAuth, providerRouter);
app.use('/topic', rateLimiter, jwtAuth, topicRouter);
app.use('/organisation', rateLimiter, jwtAuth, organisationRouter);
app.use('/translation', rateLimiter, jwtAuth, translationRouter);
app.use('/task', rateLimiter, jwtAuth, taskRouter);
app.use('/user', rateLimiter, jwtAuth, userRouter);

// admin routes
app.use('/admin', rateLimiter, jwtAuth, adminRouter);

// developer routes
app.use('/developer', rateLimiter, jwtAuth, devRouter);

app.use(errorHandler);

export default app;
