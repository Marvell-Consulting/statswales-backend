import { User } from '../../entities/user/user';
import { DatasetService } from '../../services/dataset';
import { StorageService } from '../../interfaces/storage-service';
import { Internal } from 'pechkin/dist/types.js';

declare module 'express-serve-static-core' {
  interface Request {
    user?: User;
    files?: Internal.Files;
    fileService: StorageService;
    datasetService: DatasetService;
  }
}
