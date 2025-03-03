import fs from 'fs';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { last, sortBy } from 'lodash';

import { User } from '../entities/user/user';
import { DatasetRepository } from '../repositories/dataset';
import { Locale } from '../enums/locale';
import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataLakeService } from '../services/datalake';
import { hasError, titleValidator } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { TeamSelectionDTO } from '../dtos/team-selection-dto';
import { cleanUpCube, createBaseCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE } from '../services/csv-processor';
import { createDimensionsFromSourceAssignment, validateSourceAssignment } from '../services/dimension-processor';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnDto } from '../dtos/fact-table-column-dto';
import { RevisionProviderDTO } from '../dtos/revision-provider-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { TopicDTO } from '../dtos/topic-dto';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { TopicSelectionDTO } from '../dtos/topic-selection-dto';

import { getCubePreview } from './cube-controller';

export const listAllDatasets = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const lang = req.language as Locale;
        const page = parseInt(req.query.page as string, 10) || 1;
        const limit = parseInt(req.query.limit as string, 10) || 20;
        const results = await DatasetRepository.listByLanguage(lang, page, limit);
        res.json(results);
    } catch (err) {
        logger.error(err, 'Failed to fetch dataset list');
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
        const dataset = await req.datasetService.createNew(req.body.title, req.user as User);
        res.status(201);
        res.json(DatasetDTO.fromDataset(dataset));
    } catch (err) {
        logger.error(err, `Failed to create dataset`);
        next(new UnknownException());
    }
};

export const uploadDataTable = async (req: Request, res: Response, next: NextFunction) => {
    const dataset: Dataset = res.locals.dataset;

    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    try {
        const updatedDataset = await req.datasetService.updateFactTable(dataset.id, req.file);
        const dto = DatasetDTO.fromDataset(updatedDataset);
        res.status(201);
        res.json(dto);
    } catch (err) {
        logger.error(err, 'Failed to update the fact table');
    }
};

export const cubePreview = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = dataset.draftRevision ?? last(sortBy(dataset?.revisions, 'revisionIndex'));

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
            cubeFile = await createBaseCube(dataset.id, latestRevision.id);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const start = performance.now();
    const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
    const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
    const cubePreview = await getCubePreview(cubeFile, req.language.split('-')[0], dataset, page_number, page_size);
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Generating preview of cube took ${time}ms`);
    await cleanUpCube(cubeFile);
    if ((cubePreview as ViewErrDTO).errors) {
        res.status(500);
    }
    res.json(cubePreview);
};

export const updateMetadata = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const metadata = await dtoValidator(RevisionMetadataDTO, req.body);
        const updatedDataset = await req.datasetService.updateMetadata(res.locals.datasetId, metadata);
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

export const getTasklist = async (req: Request, res: Response, next: NextFunction) => {
    const dataset: Dataset = res.locals.dataset;
    const revision = dataset.draftRevision!;
    try {
        const tasklistState = TasklistStateDTO.fromDataset(dataset, revision, req.language as Locale);
        res.json(tasklistState);
    } catch (err) {
        logger.error(err, `There was a problem fetching the tasklist for dataset ${dataset.id}`);
        next(new UnknownException('errors.tasklist_error'));
    }
};

export const getDataProviders = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    try {
        const providers = dataset.draftRevision.revisionProviders.map((provider: RevisionProvider) =>
            RevisionProviderDTO.fromRevisionProvider(provider)
        );
        res.json(providers);
    } catch (err) {
        next(err);
    }
};

export const addDataProvider = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const provider = await dtoValidator(RevisionProviderDTO, req.body);
        const updatedDataset = await req.datasetService.addDataProvider(res.locals.datasetId, provider);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        logger.error(err, 'failed to add provider');
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

export const updateDataProviders = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const providers = await arrayValidator(RevisionProviderDTO, req.body);
        const updatedDataset = await req.datasetService.updateDataProviders(res.locals.datasetId, providers);
        res.status(201);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err: any) {
        logger.error(err, 'failed to update providers');
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

export const getTopics = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const revisionTopics = res.locals.dataset?.draftRevision?.revisionTopics || [];
        const topics = revisionTopics.map((revTopic: RevisionTopic) => TopicDTO.fromTopic(revTopic.topic));
        res.json(topics);
    } catch (err) {
        next(err);
    }
};

export const updateTopics = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasetId = res.locals.datasetId;
        const datasetTopics = await dtoValidator(TopicSelectionDTO, req.body);
        const updatedDataset = await req.datasetService.updateTopics(datasetId, datasetTopics.topics);
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

export const updateSources = async (req: Request, res: Response, next: NextFunction) => {
    const dataset: Dataset = res.locals.dataset;
    const revision = dataset.draftRevision;
    const dataTable = revision?.dataTable;
    const sourceAssignment = req.body;

    if (!revision || revision.revisionIndex !== 1) {
        next(new UnknownException('errors.no_first_revision'));
        return;
    }

    if (!dataTable) {
        next(new UnknownException('errors.no_fact_table'));
        return;
    }

    logger.debug(`Processing request to update dataset sources...`);
    try {
        const validatedSourceAssignment = validateSourceAssignment(dataTable, sourceAssignment);
        await createDimensionsFromSourceAssignment(dataset, dataTable, validatedSourceAssignment);
        const updatedDataset = await DatasetRepository.getById(dataset.id);
        res.json(DatasetDTO.fromDataset(updatedDataset));
    } catch (err) {
        logger.error(err, `An error occurred trying to process the source assignments: ${err}`);

        if (err instanceof SourceAssignmentException) {
            next(new BadRequestException(err.message));
        } else {
            next(new BadRequestException('errors.invalid_source_assignment'));
        }
    }
};

export const getFactTableDefinition = async (req: Request, res: Response, next: NextFunction) => {
    const dataset: Dataset = res.locals.dataset;
    const factTableDto: FactTableColumnDto[] =
        dataset.factTable?.map((col: FactTableColumn) => FactTableColumnDto.fromFactTableColumn(col)) || [];
    res.status(200);
    res.json(factTableDto);
};
