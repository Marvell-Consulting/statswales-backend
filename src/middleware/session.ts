import RedisStore from 'connect-redis';
import session, { MemoryStore } from 'express-session';
import { createClient } from 'redis';

import { appConfig } from '../config';
import { logger } from '../utils/logger';

const config = appConfig();

const sessionLength = 24 * 60 * 60 * 1000; // 24 hours

let store: RedisStore | MemoryStore;

if (process.env.SESSION_STORE === 'redis') {
    logger.debug('Initializing Redis session store...');

    const url = process.env.REDIS_URL;
    const password = process.env.REDIS_ACCESS_KEY;
    const prefix = 'sw3b:';

    const redisClient = createClient({ url, password });

    redisClient
        .connect()
        .then(() => logger.info('Redis session store initialized'))
        .catch((err) => logger.error(err));

    store = new RedisStore({ client: redisClient, prefix });
} else {
    logger.info('In-memory session store initialized');
    store = new MemoryStore({});
}

export default session({
    secret: config.session.secret,
    name: 'statswales.backend',
    store,
    resave: false,
    saveUninitialized: false,
    proxy: true,
    cookie: {
        secure: config.session.secure
    }
});
