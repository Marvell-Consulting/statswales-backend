import { timingSafeEqual } from 'node:crypto';

import { NextFunction, Request, Response, Router } from 'express';
import passport from 'passport';
import { isString } from 'lodash';
import { Pool } from 'pg';

import { User } from '../entities/user/user';
import { dbManager } from '../db/database-manager';
import { checkDb } from '../db/db-check';
import { logger } from '../utils/logger';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { getSessionStoreStatus } from '../middleware/session';
import { StorageService } from '../interfaces/storage-service';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';
import { config } from '../config';
import { AppEnv } from '../config/env.enum';

const healthcheck = Router();

interface PoolClientsSnapshot {
  min: number;
  max: number;
  idle: number;
  waiting: number;
  expired: number;
  total: number;
  isFull: boolean;
}

interface PoolStats {
  name: string;
  connectionTimeout: string; // e.g. "5000ms"
  idleTimeout: string; // e.g. "10000ms"
  clients: PoolClientsSnapshot;
}

const dbPoolStats = (pool: Pool): PoolStats | Error => {
  if (!pool) {
    return new Error('Pool info not available');
  }

  return {
    name: pool.options.application_name ?? 'unknown',
    connectionTimeout: `${pool.options.connectionTimeoutMillis ?? 0}ms`,
    idleTimeout: `${pool.options.idleTimeoutMillis ?? 0}ms`,
    clients: {
      min: pool.options.min ?? 0,
      max: pool.options.max ?? 0,
      idle: pool.idleCount,
      waiting: pool.waitingCount,
      expired: pool.expiredCount,
      total: pool.totalCount,
      isFull: pool.options.max !== undefined ? pool.totalCount >= pool.options.max : false
    }
  };
};

const checkStorage = async (fileService: StorageService): Promise<boolean> => {
  await fileService.getServiceClient().getProperties();
  return true;
};

const timeout = (timer: number, service: string): Promise<string> =>
  new Promise((resolve) => {
    setTimeout(resolve, timer, `${service} timeout after ${timer}ms`);
  });

const checkConnections = async (req: Request, res: Response): Promise<void> => {
  const healthConfig = config.healthcheck;
  const poolStats: (PoolStats | Error)[] = [
    dbPoolStats(dbManager.getConsumerPool()),
    dbPoolStats(dbManager.getPublisherPool()),
    dbPoolStats(dbManager.getCubePool())
  ];

  try {
    // checkDb enforces its own timeouts via the pg Client so no outer race is needed.
    await checkDb();
    const storageResult = await Promise.race([
      checkStorage(req.fileService),
      timeout(healthConfig.storageTimeoutMs, 'file storage')
    ]);
    if (isString(storageResult) && storageResult.includes('timeout')) {
      throw new Error(storageResult);
    }
  } catch (err) {
    logger.error(err, `connection check failed - poolStats: ${JSON.stringify(poolStats || [])}`);
    res.status(500).json({ error: 'connection check failed' });
    return;
  }

  res.json({ message: 'success', sessionStore: getSessionStoreStatus() });
};

healthcheck.get('/', (_req: Request, res: Response) => {
  res.json({ message: 'success' }); // server is up
});

healthcheck.get('/ready', checkConnections); // server is up and has active connections to dependencies

healthcheck.get('/live', (_req: Request, res: Response) => {
  res.json({ message: 'success' }); // server is up
});

// for testing language detection / switching is working
healthcheck.get('/language', (req: Request, res: Response) => {
  res.json({ lang: req.language, supported: SUPPORTED_LOCALES });
});

// for testing jwt auth is working
healthcheck.get('/jwt', passport.authenticate('jwt', { session: false }), (req: Request, res: Response) => {
  res.json({ message: 'success', user: UserDTO.fromUser(req.user as User, req.language as Locale) });
});

// Environments where /healthcheck/db may be hit without a key, for developer convenience.
const KEYLESS_ENVS: AppEnv[] = [AppEnv.Local, AppEnv.Ci];

// Guards /healthcheck/db with a shared secret. When config.healthcheck.dbStatsKey is set, callers
// must send a matching x-healthcheck-key header (compared in constant time to avoid leaking the
// key via timing). When the key is unset the endpoint stays open in local/CI, but everywhere else
// it fails closed with a 404 — so an omitted app setting can't silently expose pool internals.
const requireDbStatsKey = (req: Request, res: Response, next: NextFunction): void => {
  const expected = config.healthcheck.dbStatsKey;

  if (!expected) {
    if (KEYLESS_ENVS.includes(config.env)) {
      next();
      return;
    }
    res.status(404).json({ error: 'not found' });
    return;
  }

  const provided = req.header('x-healthcheck-key') ?? '';
  const expectedBuf = Buffer.from(expected);
  const providedBuf = Buffer.from(provided);

  if (providedBuf.length === expectedBuf.length && timingSafeEqual(providedBuf, expectedBuf)) {
    next();
    return;
  }

  res.status(401).json({ error: 'unauthorised' });
};

healthcheck.get('/db', requireDbStatsKey, (_req: Request, res: Response) => {
  try {
    res.json({
      pools: [
        dbPoolStats(dbManager.getConsumerPool()),
        dbPoolStats(dbManager.getPublisherPool()),
        dbPoolStats(dbManager.getCubePool())
      ]
    });
  } catch (error) {
    logger.error(error, 'Error fetching database pool information');
    res.status(500).json({ error: 'Failed to fetch database pool information' });
  }
});

export const healthcheckRouter = healthcheck;
