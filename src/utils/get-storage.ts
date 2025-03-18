import { appConfig } from '../config';
import { FileStore } from '../config/file-store.enum';
import { StorageService } from '../interfaces/storage-service';
import BlobStorage from '../services/blob-storage';
import DataLakeStorage from '../services/datalake-storage';

export const getStorage = (): StorageService => {
  const config = appConfig();

  return config.storage.store === FileStore.Blob
    ? new BlobStorage(config.storage.blob)
    : new DataLakeStorage(config.storage.datalake);
};
