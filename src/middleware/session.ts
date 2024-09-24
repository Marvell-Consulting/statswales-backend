import RedisStore from 'connect-redis';
import session, { MemoryStore } from 'express-session';
import { createClient } from 'redis';

import { appConfig } from '../config';
import { logger } from '../utils/logger';
import { SessionStore } from '../config/session-store.enum';

const config = appConfig();

let store: RedisStore | MemoryStore;

if (config.session.store === SessionStore.REDIS) {
    logger.debug('Initializing Redis session store...');

    const redisClient = createClient({ url: config.session.redisUrl, password: config.session.redisPassword });

    redisClient
        .connect()
        .then(() => logger.info('Redis session store initialized'))
        .catch((err) => logger.error(err));

    store = new RedisStore({ client: redisClient, prefix: 'sw3b:' });
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
        secure: config.session.secure,
        maxAge: config.session.maxAge
    }
});
