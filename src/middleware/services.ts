import { NextFunction, Request, Response } from 'express';

import { Locale } from '../enums/locale';
import { DatasetService } from '../services/dataset';
import { getFileService } from '../utils/get-file-service';

// initialise any request-scoped services required by the app and store them on the request object for later use
// see @types/express/index.d.ts for details
export const initServices = (req: Request, res: Response, next: NextFunction): void => {
  req.fileService = getFileService();
  req.datasetService = new DatasetService(req.language as Locale);
  next();
};
