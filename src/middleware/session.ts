import { RedisStore } from 'connect-redis';
import session, { MemoryStore, Store } from 'express-session';
import { NextFunction, Request, Response } from 'express';
import { createClient } from 'redis';

import { config } from '../config';
import { logger } from '../utils/logger';
import { SessionStore } from '../config/session-store.enum';

let store: Store;
let redisClient: ReturnType<typeof createClient> | undefined;
const usingRedis = config.session.store === SessionStore.Redis;

if (usingRedis) {
  logger.debug('Initializing Redis session store...');

  redisClient = createClient({
    url: config.session.redisUrl,
    password: config.session.redisPassword,
    disableOfflineQueue: true,
    pingInterval: 1000,
    socket: {
      reconnectStrategy: 1000,
      connectTimeout: 7500,
      family: 4
    }
  });

  logger.debug(`Connecting to redis server: ${config.session.redisUrl}`);

  redisClient.on('connect', () => logger.info('Redis session store initialized'));
  redisClient.on('error', (err) => logger.error(err, `Redis error`));
  redisClient.connect();

  store = new RedisStore({ client: redisClient, prefix: 'sw3b:' });
} else {
  logger.info('In-memory session store initialized');
  store = new MemoryStore({});
}

const sessionMiddleware = session({
  secret: config.session.secret,
  name: 'statswales.backend',
  store,
  resave: false,
  saveUninitialized: false,
  proxy: true,
  cookie: {
    secure: config.session.secure,
    maxAge: config.session.maxAge
  }
});

export interface SessionStoreStatus {
  type: 'redis' | 'memory';
  connected: boolean;
}

export const getSessionStoreStatus = (): SessionStoreStatus => {
  return {
    type: usingRedis ? 'redis' : 'memory',
    connected: redisClient ? redisClient.isReady : true
  };
};

export default (req: Request, res: Response, next: NextFunction): void => {
  sessionMiddleware(req, res, (err?: unknown) => {
    if (err) {
      const sessionError = Object.assign(new Error('errors.session_store_unavailable'), {
        status: 503,
        cause: err
      });
      return next(sessionError);
    }
    next();
  });
};
