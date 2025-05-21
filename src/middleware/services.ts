import { NextFunction, Request, Response } from 'express';

import { Locale } from '../enums/locale';
import { DatasetService } from '../services/dataset';
import { getFileService } from '../utils/get-file-service';
import { Pool } from 'pg';
import { appConfig } from '../config';

// initialise any request-scoped services required by the app and store them on the request object for later use
// see @types/express/index.d.ts for details
export const initServices = async (req: Request, res: Response, next: NextFunction): void => {
  req.fileService = getFileService();
  req.datasetService = new DatasetService(req.language as Locale);
  req.pool = await new Pool({
    database: appConfig().database.database,
    user: appConfig().database.username,
    password: appConfig().database.password,
    port: appConfig().database.port,
    ssl: appConfig().database.ssl,
    max: 20, // set pool max size to 20
    idleTimeoutMillis: 1000, // close idle clients after 1 second
    connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
    maxUses: 7500 // close (and replace) a connection after it has been used 7500 times (see below for discussion)
  }).connect();
  next();
};
