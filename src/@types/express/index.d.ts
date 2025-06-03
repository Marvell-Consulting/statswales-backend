import { User } from '../../entities/user/user';
import { DatasetService } from '../../services/dataset';
import { StorageService } from '../../interfaces/storage-service';

declare module 'express-serve-static-core' {
  interface Request {
    user?: User;
    fileService: StorageService;
    datasetService: DatasetService;
  }
}
