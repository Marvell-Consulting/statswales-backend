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

// Exported for unit testing. The `req` passed in is the pino-http-wrapped Express
// request, so all Express extensions (query, params, augmented fields) are present.
export interface SerializedRequest {
  method?: unknown;
  url?: unknown;
  query?: unknown;
  params?: unknown;
  ip?: unknown;
  rateLimitBypass: boolean;
}

export function serializeRequest(
  req: { rateLimitBypass?: boolean; ip?: string } & Record<string, unknown>
): SerializedRequest {
  return {
    ...pick(req, ['method', 'url', 'query', 'params', 'ip']),
    // Set by the rateLimiter middleware when a valid x-rate-limit-bypass token
    // was presented — i.e. the request came from one of our own frontend pods.
    // The token value itself is never logged; only its validity as a boolean.
    rateLimitBypass: Boolean(req.rateLimitBypass)
    // `ip` is Express's resolved client IP, derived from X-Forwarded-For via
    // `app.set('trust proxy', 1)`. Correct so long as the deployment topology
    // stays as Front Door → Container Apps ingress → Express (the only XFF-appending
    // hop is Front Door; the ACA ingress is transparent and locked to Front Door
    // egress IPs). If another L7 layer is ever added in front (e.g. App Gateway),
    // bump `trust proxy` to match the new hop count or this field will log the
    // intermediate proxy's address instead of the real client.
  };
}

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
    req: serializeRequest,
    res(res) {
      return pick(res, ['statusCode']);
    }
  }
});
