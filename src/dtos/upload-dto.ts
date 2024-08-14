/* eslint-disable import/no-cycle */
import { Error } from '../models/error';

import { DatasetDTO } from './dataset-dto';

export interface UploadDTO {
    success: true;
    dataset: DatasetDTO;
}

export interface UploadErrDTO {
    success: boolean;
    dataset: DatasetDTO | undefined;
    errors: Error[];
}
