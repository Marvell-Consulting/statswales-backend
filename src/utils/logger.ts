import pino from 'pino';
import pinoHttp from 'pino-http';
import pick from 'lodash/pick';

import { appConfig } from '../config';

const config = appConfig();

export const logger = pino({
    level: config.logger.level
});

export const httpLogger = pinoHttp({
    logger,
    autoLogging: {
        ignore: (req) => {
            const ignorePathsRx = /^\/css|\/public|\/assets|\/favicon/;
            return ignorePathsRx.test(req.url || '');
        }
    },
    customLogLevel(req, res, err) {
        if (res.statusCode >= 400 && res.statusCode < 500) {
            return 'warn';
        } else if (res.statusCode >= 500 || err) {
            return 'error';
        }
        return 'info';
    },
    serializers: {
        req(req) {
            return pick(req, ['method', 'url', 'query', 'params']);
        },
        res(res) {
            return pick(res, ['statusCode']);
        }
    }
});
