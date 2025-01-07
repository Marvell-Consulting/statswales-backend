import fs from 'fs';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';

import { DatasetRepository } from '../repositories/dataset';
import { Locale } from '../enums/locale';
import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataLakeService } from '../services/datalake';
import { hasError, titleValidator } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { User } from '../entities/user/user';
import { FactTable } from '../entities/dataset/fact-table';
import { FactTableAction } from '../enums/fact-table-action';
import { RevisionRepository } from '../repositories/revision';
import { getLatestRevision } from '../utils/latest';
import { ViewErrDTO } from '../dtos/view-dto';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { DatasetInfoDTO } from '../dtos/dataset-info-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { DatasetProviderDTO } from '../dtos/dataset-provider-dto';
import { TopicSelectionDTO } from '../dtos/topic-selection-dto';
import { TeamSelectionDTO } from '../dtos/team-selection-dto';
import { createBaseCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';

import { getCubePreview } from './cube-controller';

export const listAllDatasets = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasets = await DatasetRepository.listAllByLanguage(req.language as Locale);
        res.json({ datasets });
    } catch (err) {
        logger.error('Failed to fetch dataset list:', err);
        next(new UnknownException());
    }
};

export const listActiveDatasets = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasets = await DatasetRepository.listActiveByLanguage(req.language as Locale);
        res.json({ datasets });
    } catch (err) {
        logger.error('Failed to fetch active dataset list:', err);
        next(new UnknownException());
    }
};

export const getDatasetById = async (req: Request, res: Response, next: NextFunction) => {
    res.json(DatasetDTO.fromDataset(res.locals.dataset));
};

export const deleteDatasetById = async (req: Request, res: Response, next: NextFunction) => {
    const dataLakeService = new DataLakeService();
    await dataLakeService.deleteDirectoryAndFiles(req.params.dataset_id);
    await DatasetRepository.deleteById(res.locals.datasetId);
    res.status(204);
    res.end();
};

export const createDataset = async (req: Request, res: Response, next: NextFunction) => {
    const titleError = await hasError(titleValidator(), req);
    if (titleError) {
        next(new BadRequestException('errors.no_title'));
        return;
    }

    try {
        const language = req.language as Locale;
        const dataset = await DatasetRepository.createWithTitle(req.user as User, language, req.body.title);
        logger.info(`Dataset created with id: ${dataset.id}`);
        res.status(201);
        res.json(DatasetDTO.fromDataset(dataset));
    } catch (err) {
        logger.error(`An error occurred trying to create a dataset: ${err}`);
        next(new UnknownException());
    }
};

export const createFirstRevision = async (req: Request, res: Response, next: NextFunction) => {
    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    const utf8Buffer = convertBufferToUTF8(req.file.buffer);

    let fileImport: FactTable;
    logger.debug('Uploading dataset to datalake');
    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, res.locals.datasetId);
        fileImport.action = FactTableAction.Add;
    } catch (err) {
        logger.error(`An error occurred trying to upload the file: ${err}`);
        next(new UnknownException('errors.upload_error'));
        return;
    }

    logger.debug('Updating dataset records');
    try {
        const user = req.user as User;
        await RevisionRepository.createFromImport(res.locals.dataset, fileImport, user);
        const dataset = await DatasetRepository.getById(res.locals.datasetId);
        res.status(201);
        res.json(DatasetDTO.fromDataset(dataset));
    } catch (err) {
        logger.error(`An error occurred trying to create a revision: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};

export const cubePreview = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = getLatestRevision(dataset);

    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
    const cubePreview = await getCubePreview(cubeFile, req.language.split('-')[0], dataset, page_number, page_size);

    if ((cubePreview as ViewErrDTO).errors) {
        res.status(500);
    }
    res.json(cubePreview);
};

export const updateDatasetInfo = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const infoDto = await dtoValidator(DatasetInfoDTO, req.body);
        const updatedDataset = await DatasetRepository.patchInfoById(res.locals.datasetId, infoDto);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        if (err instanceof BadRequestException) {
            err.validationErrors?.forEach((error) => {
                if (!error.constraints) return;
                Object.values(error.constraints).forEach((message) => logger.error(message));
            });
            next(err);
            return;
        }
        next(new UnknownException('errors.info_update_error'));
    }
};

export const getDatasetTasklist = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const tasklistState = TasklistStateDTO.fromDataset(res.locals.dataset, req.language as Locale);
        res.json(tasklistState);
    } catch (err) {
        next(new UnknownException('errors.tasklist_error'));
    }
};

export const addProvidersToDataset = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasetId = res.locals.datasetId;
        const provider = await dtoValidator(DatasetProviderDTO, req.body);
        const updatedDataset = await DatasetRepository.addDatasetProvider(datasetId, provider);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        if (err instanceof BadRequestException) {
            err.validationErrors?.forEach((error) => {
                if (!error.constraints) return;
                Object.values(error.constraints).forEach((message) => logger.error(message));
            });
            next(err);
            return;
        }
        next(new UnknownException('errors.provider_update_error'));
    }
};

export const updateDatasetProviders = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasetId = res.locals.datasetId;
        const providers = await arrayValidator(DatasetProviderDTO, req.body);
        const updatedDataset = await DatasetRepository.updateDatasetProviders(datasetId, providers);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        if (err instanceof BadRequestException) {
            err.validationErrors?.forEach((error) => {
                if (!error.constraints) return;
                Object.values(error.constraints).forEach((message) => logger.error(message));
            });
            next(err);
            return;
        }
        next(new UnknownException('errors.provider_update_error'));
    }
};

export const updateDatasetTopics = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasetId = res.locals.datasetId;
        const datasetTopics = await dtoValidator(TopicSelectionDTO, req.body);
        const updatedDataset = await DatasetRepository.updateDatasetTopics(datasetId, datasetTopics.topics);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        if (err instanceof BadRequestException) {
            err.validationErrors?.forEach((error) => {
                if (!error.constraints) return;
                Object.values(error.constraints).forEach((message) => logger.error(message));
            });
            next(err);
            return;
        }
        next(new UnknownException('errors.topic_update_error'));
    }
};

export const updateDatasetTeam = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasetId = res.locals.datasetId;
        const datasetTeam = await dtoValidator(TeamSelectionDTO, req.body);
        const updatedDataset = await DatasetRepository.updateDatasetTeam(datasetId, datasetTeam.team_id);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        if (err instanceof BadRequestException) {
            err.validationErrors?.forEach((error) => {
                if (!error.constraints) return;
                Object.values(error.constraints).forEach((message) => logger.error(message));
            });
            next(err);
            return;
        }
        next(new UnknownException('errors.topic_update_error'));
    }
};
