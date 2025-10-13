import { config } from '../config';
import { AppEnv } from '../config/env.enum';
import { FileStore } from '../config/file-store.enum';
import { StorageService } from '../interfaces/storage-service';
import BlobStorage from '../services/blob-storage';
import DataLakeStorage from '../services/datalake-storage';

let fileService: StorageService | undefined;

export const getFileService = (): StorageService => {
  if (!fileService || config.env === AppEnv.Ci) {
    fileService =
      config.storage.store === FileStore.Blob
        ? new BlobStorage(config.storage.blob)
        : new DataLakeStorage(config.storage.datalake);
  }

  return fileService;
};
