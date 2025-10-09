import { Request, Response, Router } from 'express';
import passport from 'passport';
import { isString } from 'lodash';

import { User } from '../entities/user/user';
import { dbManager } from '../db/database-manager';
import { logger } from '../utils/logger';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { StorageService } from '../interfaces/storage-service';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';
import { config } from '../config';

const healthcheck = Router();

const checkAppDb = async (): Promise<boolean> => {
  await dbManager.getAppDataSource().manager.query('SELECT 1 AS connected');
  return true;
};

const checkCubeDb = async (): Promise<boolean> => {
  await dbManager.getCubeDataSource().manager.query('SELECT 1 AS connected');
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

  try {
    const results = await Promise.all([
      Promise.race([checkAppDb(), timeout(healthConfig.dbTimeoutMs, 'app-db')]),
      Promise.race([checkCubeDb(), timeout(healthConfig.dbTimeoutMs, 'cube-db')]),
      Promise.race([checkStorage(req.fileService), timeout(healthConfig.storageTimeoutMs, 'file storage')])
    ]);
    results.forEach((result) => {
      if (isString(result) && result.includes('timeout')) throw new Error(result);
    });
  } catch (err) {
    logger.error(err, 'Healthcheck failed');
    res.status(500).json({ error: 'service down' });
    return;
  }

  res.json({ message: 'success' });
};

healthcheck.get('/', (req: Request, res: Response) => {
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

healthcheck.get('/db', async (req: Request, res: Response) => {
  try {
    const appPool = dbManager.getAppPool();
    const cubePool = dbManager.getCubePool();

    if (!appPool || !cubePool) {
      throw new Error('Database pools are not available');
    }

    res.json({
      appPool: {
        name: appPool.options.application_name,
        connectionTimeout: `${appPool.options.connectionTimeoutMillis}ms`,
        idleTimeout: `${appPool.options.idleTimeoutMillis}ms`,
        clients: {
          min: appPool.options.min,
          max: appPool.options.max,
          idle: appPool.idleCount,
          waiting: appPool.waitingCount,
          expired: appPool.expiredCount,
          total: appPool.totalCount,
          isFull: appPool.totalCount >= appPool.options.max
        }
      },
      cubePool: {
        name: cubePool.options.application_name,
        connectionTimeout: `${cubePool.options.connectionTimeoutMillis}ms`,
        idleTimeout: `${cubePool.options.idleTimeoutMillis}ms`,
        clients: {
          min: cubePool.options.min,
          max: cubePool.options.max,
          idle: cubePool.idleCount,
          waiting: cubePool.waitingCount,
          expired: cubePool.expiredCount,
          total: cubePool.totalCount,
          isFull: cubePool.totalCount >= cubePool.options.max
        }
      }
    });
  } catch (error) {
    logger.error(error, 'Error fetching database pool information');
    res.status(500).json({ error: 'Failed to fetch database pool information' });
  }
});

export const healthcheckRouter = healthcheck;
