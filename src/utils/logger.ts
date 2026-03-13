import fs from 'node:fs';

import pino from 'pino';
import pinoHttp from 'pino-http';
import pick from 'lodash/pick';

import { config } from '../config';

function createLogger(): pino.Logger {
  const logFile = process.env.LOG_FILE;
  if (logFile) {
    const streams: pino.StreamEntry[] = [
      { stream: process.stdout },
      { stream: fs.createWriteStream(logFile, { flags: 'a' }) }
    ];
    return pino({ level: config.logger.level }, pino.multistream(streams));
  }
  return pino({ level: config.logger.level });
}

export const logger = createLogger();

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
