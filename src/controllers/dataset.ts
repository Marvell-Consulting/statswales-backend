import fs from 'fs';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { last, sortBy } from 'lodash';

import { DatasetRepository } from '../repositories/dataset';
import { Locale } from '../enums/locale';
import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataLakeService } from '../services/datalake';
import { hasError, titleValidator } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { User } from '../entities/user/user';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { RevisionRepository } from '../repositories/revision';
import { getLatestRevision } from '../utils/latest';
import { ViewErrDTO } from '../dtos/view-dto';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { DatasetInfoDTO } from '../dtos/dataset-info-dto';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { DatasetProviderDTO } from '../dtos/dataset-provider-dto';
import { TopicSelectionDTO } from '../dtos/topic-selection-dto';
import { TeamSelectionDTO } from '../dtos/team-selection-dto';
import { cleanUpCube, createBaseCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, uploadCSV } from '../services/csv-processor';
import { convertBufferToUTF8 } from '../utils/file-utils';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import {
    createDimensionsFromSourceAssignment,
    ValidatedSourceAssignment,
    validateSourceAssignment
} from '../services/dimension-processor';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { Revision } from '../entities/dataset/revision';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableColumnDto } from '../dtos/fact-table-column-dto';
import { TopicDTO } from '../dtos/topic-dto';

import { getCubePreview } from './cube-controller';

export const listAllDatasets = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const datasets: DatasetListItemDTO[] = await DatasetRepository.listAllByLanguage(req.language as Locale);
        res.json(datasets);
    } catch (err) {
        logger.error(err, 'Failed to fetch dataset list');
        next(new UnknownException());
    }
};

export const listActiveDatasets = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const lang = req.language as Locale;
        const page = parseInt(req.query.page as string, 10) || 1;
        const limit = parseInt(req.query.limit as string, 10) || 20;

        const results: ResultsetWithCount<DatasetListItemDTO> = await DatasetRepository.listActiveByLanguage(
            lang,
            page,
            limit
        );

        res.json(results);
    } catch (err) {
        logger.error(err, 'Failed to fetch active dataset list');
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
    const dataset: Dataset = res.locals.dataset;

    if (!req.file) {
        next(new BadRequestException('errors.upload.no_csv'));
        return;
    }

    const utf8Buffer = convertBufferToUTF8(req.file.buffer);

    let fileImport: DataTable;
    logger.debug('Uploading dataset to datalake');
    try {
        fileImport = await uploadCSV(utf8Buffer, req.file?.mimetype, req.file?.originalname, res.locals.datasetId);
        fileImport.action = DataTableAction.ReplaceAll;
        fileImport.dataTableDescriptions.forEach((col) => {
            col.factTableColumn = col.columnName;
        });
    } catch (err) {
        logger.error(`An error occurred trying to upload the file: ${err}`);
        next(new UnknownException('errors.upload_error'));
        return;
    }

    if (dataset.factTable && dataset.factTable.length > 0) {
        await FactTableColumn.getRepository().remove(dataset.factTable);
    }

    logger.debug('Updating dataset records');
    try {
        const user = req.user as User;
        await RevisionRepository.createFromImport(res.locals.dataset, fileImport, user);
        logger.debug('Creating base fact table definition');
        logger.debug(`Creating fact table definitions for dataset ${res.locals.dataset.id}`);
        for (const fileImportCol of fileImport.dataTableDescriptions) {
            const factTable = new FactTableColumn();
            factTable.id = res.locals.dataset.id;
            factTable.columnName = fileImportCol.columnName;
            factTable.columnIndex = fileImportCol.columnIndex;
            factTable.columnDatatype = fileImportCol.columnDatatype;
            factTable.columnType = FactTableColumnType.Unknown;
            logger.debug(`Creating fact table definition for column ${fileImportCol.columnName}`);
            await factTable.save();
        }
        const dataset = await DatasetRepository.getById(res.locals.dataset.id);
        logger.debug(`Producing DTO for dataset ${dataset.id}`);
        const dto = DatasetDTO.fromDataset(dataset);
        res.status(201);
        res.json(dto);
    } catch (err) {
        logger.error(`An error occurred trying to create a revision: ${err}`);
        next(new UnknownException('errors.upload_error'));
    }
};

export const cubePreview = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = last(sortBy(dataset?.revisions, 'revisionIndex'));

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

export const getDatasetProviders = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const relations = { datasetProviders: { provider: true, providerSource: true } };
        const dataset: Dataset = await DatasetRepository.getById(res.locals.datasetId, relations);
        const providers = dataset.datasetProviders.map((provider) => DatasetProviderDTO.fromDatasetProvider(provider));
        res.json(providers);
    } catch (err) {
        next(err);
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

export const getDatasetTopics = async (req: Request, res: Response, next: NextFunction) => {
    try {
        const dataset = await DatasetRepository.getById(res.locals.datasetId, { datasetTopics: { topic: true } });
        const topics = dataset.datasetTopics.map((datasetTopic) => TopicDTO.fromTopic(datasetTopic.topic));
        res.json(topics);
    } catch (err) {
        next(err);
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

async function updateFactTableDefinition(
    dataset: Dataset,
    dataTable: DataTable,
    validatedSourceAssignment: ValidatedSourceAssignment
) {
    const factTableDef: FactTableColumn[] = [];
    if (validatedSourceAssignment.dataValues) {
        const dataTableColumn = dataTable.dataTableDescriptions.find(
            (column) => column.columnName === validatedSourceAssignment.dataValues?.column_name
        );
        const factTable = new FactTableColumn();
        factTable.dataset = dataset;
        factTable.columnName = validatedSourceAssignment.dataValues.column_name;
        factTable.columnType = FactTableColumnType.DataValues;
        factTable.columnIndex = validatedSourceAssignment.dataValues.column_index;
        factTable.columnDatatype = dataTableColumn?.columnDatatype || 'varchar';
        const savedFactTable = await factTable.save();
        factTableDef.push(savedFactTable);
    }
    if (validatedSourceAssignment.measure) {
        const dataTableColumn = dataTable.dataTableDescriptions.find(
            (column) => column.columnName === validatedSourceAssignment.measure?.column_name
        );
        const factTable = new FactTableColumn();
        factTable.dataset = dataset;
        factTable.columnName = validatedSourceAssignment.measure.column_name;
        factTable.columnType = FactTableColumnType.Measure;
        factTable.columnIndex = validatedSourceAssignment.measure.column_index;
        factTable.columnDatatype = dataTableColumn?.columnDatatype || 'varchar';
        const savedFactTable = await factTable.save();
        factTableDef.push(savedFactTable);
    }
    if (validatedSourceAssignment.noteCodes) {
        const dataTableColumn = dataTable.dataTableDescriptions.find(
            (column) => column.columnName === validatedSourceAssignment.noteCodes?.column_name
        );
        const factTable = new FactTableColumn();
        factTable.dataset = dataset;
        factTable.columnName = validatedSourceAssignment.noteCodes.column_name;
        factTable.columnType = FactTableColumnType.NoteCodes;
        factTable.columnIndex = validatedSourceAssignment.noteCodes.column_index;
        factTable.columnDatatype = dataTableColumn?.columnDatatype || 'varchar';
        const savedFactTable = await factTable.save();
        factTableDef.push(savedFactTable);
    }
    if (validatedSourceAssignment.dimensions) {
        for (const dimension of validatedSourceAssignment.dimensions) {
            const dataTableColumn = dataTable.dataTableDescriptions.find(
                (column) => column.columnName === dimension.column_name
            );
            const factTable = new FactTableColumn();
            factTable.dataset = dataset;
            factTable.columnName = dimension.column_name;
            factTable.columnType = FactTableColumnType.Dimension;
            factTable.columnIndex = dimension.column_index;
            factTable.columnDatatype = dataTableColumn?.columnDatatype || 'varchar';
            const savedFactTable = await factTable.save();
            factTableDef.push(savedFactTable);
        }
    }
    return factTableDef;
}

export const updateSources = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    logger.debug(`Req Body = ${JSON.stringify(req.body, null, 2)}`);
    const sourceAssignment = req.body;
    const revision = dataset.revisions.find((revision: Revision) => revision.revisionIndex === 1);
    if (!revision) {
        next(new UnknownException('errors.no_first_revision'));
        return;
    }
    const dataTable = revision.dataTable;
    if (!dataTable) {
        next(new UnknownException('errors.no_fact_table'));
        return;
    }
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
    const { dataset } = res.locals;
    const factTableDto: FactTableColumnDto[] =
        dataset.factTable?.map((col: FactTableColumn) => FactTableColumnDto.fromFactTableColumn(col)) || [];
    res.status(200);
    res.json(factTableDto);
};
