import { Request, Response, Router } from 'express';
import passport from 'passport';
import { isString } from 'lodash';

import { User } from '../entities/user/user';
import { dbManager } from '../db/database-manager';
import { logger } from '../utils/logger';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { getSessionStoreStatus } from '../middleware/session';
import { StorageService } from '../interfaces/storage-service';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';
import { config } from '../config';
import { Client, Pool } from 'pg';

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

// Use a one-off connection rather than the pool so a saturated pool can't make
// the readiness probe hang past Azure's probe timeout.
const checkDb = async (): Promise<boolean> => {
  const client = new Client({
    host: config.database.host,
    port: config.database.port,
    user: config.database.username,
    password: config.database.password,
    database: config.database.database,
    ssl: config.database.ssl,
    application_name: 'sw3-backend-healthcheck',
    connectionTimeoutMillis: config.healthcheck.dbTimeoutMs
  });

  try {
    await client.connect();
    await client.query('SELECT 1 AS connected');
  } finally {
    await client.end().catch(() => undefined);
  }
  return true;
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
  const poolStats: (PoolStats | Error)[] = [dbPoolStats(dbManager.getAppPool()), dbPoolStats(dbManager.getCubePool())];

  try {
    const results = await Promise.all([
      Promise.race([checkDb(), timeout(healthConfig.dbTimeoutMs, 'db')]),
      Promise.race([checkStorage(req.fileService), timeout(healthConfig.storageTimeoutMs, 'file storage')])
    ]);
    results.forEach((result) => {
      if (isString(result) && result.includes('timeout')) throw new Error(result);
    });
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

healthcheck.get('/db', (_req: Request, res: Response) => {
  try {
    res.json({
      appPool: dbPoolStats(dbManager.getAppPool()),
      cubePool: dbPoolStats(dbManager.getCubePool())
    });
  } catch (error) {
    logger.error(error, 'Error fetching database pool information');
    res.status(500).json({ error: 'Failed to fetch database pool information' });
  }
});

export const healthcheckRouter = healthcheck;
