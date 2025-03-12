import { Request, Response, Router } from 'express';
import passport from 'passport';
import { isString } from 'lodash';

import { sanitiseUser } from '../utils/sanitise-user';
import { User } from '../entities/user/user';
import { appConfig } from '../config';
import { AppEnv } from '../config/env.enum';
import { DataLakeService } from '../services/datalake';
import { dataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { SUPPORTED_LOCALES } from '../middleware/translation';

const config = appConfig();
const healthcheck = Router();

const checkDb = async (): Promise<boolean> => {
  await dataSource.manager.query('SELECT 1 AS connected');
  return true;
};

const checkDatalake = async (): Promise<boolean> => {
  if (config.env !== AppEnv.Ci) {
    const datalake = new DataLakeService();
    await datalake.getServiceClient().getProperties();
  }
  return true;
};

const timeout = (timer: number, service: string) =>
  new Promise((resolve) => {
    setTimeout(resolve, timer, `${service} timeout`);
  });

const stillAlive = async (req: Request, res: Response) => {
  try {
    const timeoutMs = 1000;
    const results = await Promise.all([
      Promise.race([checkDb(), timeout(timeoutMs, 'db')]),
      Promise.race([checkDatalake(), timeout(timeoutMs, 'datalake')])
    ]);
    results.forEach((result) => {
      if (isString(result) && result.includes('timeout')) throw new Error(`${result} after ${timeoutMs}ms`);
    });
  } catch (err) {
    logger.error(err, 'Healthcheck failed');
    res.status(500).json({ error: 'service down' });
    return;
  }

  res.json({ message: 'success' }); // server is up and has connection to db and datalake
};

healthcheck.get('/', (req: Request, res: Response) => {
  res.json({ message: 'success' }); // server is up
});

healthcheck.get('/ready', stillAlive);
healthcheck.get('/live', stillAlive);

// for testing language detection / switching is working
healthcheck.get('/language', (req, res) => {
  res.json({ lang: req.language, supported: SUPPORTED_LOCALES });
});

// for testing jwt auth is working
healthcheck.get('/jwt', passport.authenticate('jwt', { session: false }), (req, res) => {
  res.json({ message: 'success', user: sanitiseUser(req.user as User) });
});

export const healthcheckRouter = healthcheck;
