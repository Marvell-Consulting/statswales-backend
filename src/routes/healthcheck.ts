import { Request, Response, Router } from 'express';
import passport from 'passport';
import { isString } from 'lodash';

import { User } from '../entities/user/user';
import { appDataSource } from '../db/data-source';
import { logger } from '../utils/logger';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { StorageService } from '../interfaces/storage-service';
import { Locale } from '../enums/locale';
import { UserDTO } from '../dtos/user/user-dto';

const healthcheck = Router();

const checkDb = async (): Promise<boolean> => {
  await appDataSource.manager.query('SELECT 1 AS connected');
  return true;
};

const checkStorage = async (fileService: StorageService): Promise<boolean> => {
  await fileService.getServiceClient().getProperties();
  return true;
};

const timeout = (timer: number, service: string): Promise<string> =>
  new Promise((resolve) => {
    setTimeout(resolve, timer, `${service} timeout`);
  });

const stillAlive = async (req: Request, res: Response): Promise<void> => {
  try {
    const timeoutMs = 1000;
    const results = await Promise.all([
      Promise.race([checkDb(), timeout(timeoutMs, 'db')]),
      Promise.race([checkStorage(req.fileService), timeout(timeoutMs, 'file storage')])
    ]);
    results.forEach((result) => {
      if (isString(result) && result.includes('timeout')) throw new Error(`${result} after ${timeoutMs}ms`);
    });
  } catch (err) {
    logger.error(err, 'Healthcheck failed');
    res.status(500).json({ error: 'service down' });
    return;
  }

  res.json({ message: 'success' }); // server is up and has connection to db and file storage
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
  res.json({ message: 'success', user: UserDTO.fromUser(req.user as User, req.language as Locale) });
});

export const healthcheckRouter = healthcheck;
