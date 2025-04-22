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
import { userGroupIdValidator, hasError, titleValidator } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { cleanUpCube, createBaseCube, createBaseCubeFromProtoCube } from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE } from '../services/csv-processor';
import {
  createDimensionsFromSourceAssignment,
  ValidatedSourceAssignment,
  validateSourceAssignment
} from '../services/dimension-processor';
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
import { factTableValidatorFromSource } from '../services/fact-table-validator';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import JSZip from 'jszip';
import { addDirectoryToZip, collectFiles } from '../utils/dataset-controller-utils';
import { t } from 'i18next';
import { NotAllowedException } from '../exceptions/not-allowed.exception';

export const listUserDatasets = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const user = req.user as User;
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 20;
    const results = await DatasetRepository.listForUser(user, lang, page, limit);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch dataset list');
    next(new UnknownException());
  }
};

export const getDatasetById = async (req: Request, res: Response) => {
  res.json(DatasetDTO.fromDataset(res.locals.dataset));
};

export const deleteDraftDatasetById = async (req: Request, res: Response, next: NextFunction) => {
  const dataset: Dataset = res.locals.dataset;
  if (dataset.publishedRevision) {
    next(new NotAllowedException('Dataset is already published, cannot delete'));
    return;
  }
  await req.fileService.deleteDirectory(req.params.dataset_id);
  await DatasetRepository.deleteById(res.locals.datasetId);
  res.status(202);
  res.end();
};

export const createDataset = async (req: Request, res: Response, next: NextFunction) => {
  const titleError = await hasError(titleValidator(), req);
  if (titleError) {
    next(new BadRequestException('errors.no_title'));
    return;
  }

  const validUserGroupIds = req.user?.groupRoles.map((gr) => gr.groupId) as string[];
  const groupIdError = await hasError(userGroupIdValidator(validUserGroupIds), req);
  if (groupIdError) {
    next(new BadRequestException('errors.user_group_id.invalid'));
    return;
  }

  try {
    const { title, user_group_id } = req.body;
    const dataset = await req.datasetService.createNew(title, user_group_id, req.user as User);
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
    res.status(500);
    const error: ViewErrDTO = {
      status: 500,
      dataset_id: dataset.id,
      errors: [
        {
          field: 'csv',
          message: {
            key: 'errors.unknown_error',
            params: {}
          },
          user_message: [
            {
              lang: req.language,
              message: t('errors.unknown_error', { lng: req.language })
            }
          ]
        }
      ]
    };
    res.json(error);
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
  if (latestRevision.onlineCubeFilename && !latestRevision.onlineCubeFilename.includes('protocube')) {
    logger.debug('Loading cube from file store for preview');
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    try {
      const cubeBuffer = await req.fileService.loadBuffer(latestRevision.onlineCubeFilename, dataset.id);
      fs.writeFileSync(cubeFile, cubeBuffer);
    } catch (err) {
      logger.error('Something went wrong trying to download file from data lake');
      throw err;
    }
  } else if (latestRevision.onlineCubeFilename && latestRevision.onlineCubeFilename.includes('protocube')) {
    logger.debug('Loading protocube from file store for preview');
    const buffer = await req.fileService.loadBuffer(latestRevision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, buffer);
    await createBaseCubeFromProtoCube(dataset.id, latestRevision.id, cubeFile);
  } else {
    logger.debug('Creating fresh cube for preview... This could take a few seconds');
    try {
      cubeFile = await createBaseCube(dataset.id, latestRevision.id);
    } catch (error) {
      logger.error(error, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
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
  } catch (err: unknown) {
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
  const dataset = res.locals.dataset;
  try {
    const tasklistState = await req.datasetService.getTasklistState(dataset.id, req.language as Locale);
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
  } catch (err: unknown) {
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
  } catch (err: unknown) {
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
  } catch (err: unknown) {
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

  if (!sourceAssignment) {
    next(new BadRequestException('Could not assign source types to import'));
    return;
  }

  if (!revision || revision.revisionIndex !== 1) {
    next(new UnknownException('errors.no_first_revision'));
    return;
  }

  if (!dataTable) {
    next(new UnknownException('errors.no_fact_table'));
    return;
  }

  logger.debug(`Processing request to update dataset sources...`);
  let validatedSourceAssignment: ValidatedSourceAssignment;
  try {
    validatedSourceAssignment = validateSourceAssignment(dataTable, sourceAssignment);
  } catch (err) {
    const error = err as SourceAssignmentException;
    res.status(error.status);
    res.json({
      status: error.status,
      dataset_id: dataset.id,
      errors: [
        {
          field: 'none',
          message: {
            key: error.message
          }
        }
      ]
    });
    return;
  }
  let duckdbFile: string;
  try {
    duckdbFile = await factTableValidatorFromSource(dataset, validatedSourceAssignment);
  } catch (err) {
    const error = err as FactTableValidationException;
    res.status(error.status);
    res.json({
      status: error.status,
      dataset_id: dataset.id,
      errors: [
        {
          field: 'none',
          message: {
            key: error.tag,
            params: {}
          }
        }
      ],
      data: error.data,
      headers: error.headers
    });
    return;
  }
  try {
    logger.debug(`Saving duckdb file to blob storage: ${duckdbFile}`);
    const buffer = fs.readFileSync(duckdbFile);
    logger.debug(`Buffer size: ${buffer.byteLength} bytes`);
    await req.fileService.saveBuffer(`${revision.id}-protocube.duckdb`, dataset.id, buffer);
    revision.onlineCubeFilename = `${revision.id}-protocube.duckdb`;
    fs.unlinkSync(duckdbFile);
    await revision.save();
  } catch (err) {
    logger.error(err, 'Failed to save duckdb file to blob storage');
    res.status(500);
    res.json({
      status: 500,
      dataset_id: dataset.id,
      errors: [
        {
          field: 'none',
          message: {
            key: 'errors.fact_table_validation.unknown_error',
            params: {}
          }
        }
      ]
    });
    return;
  }

  try {
    await createDimensionsFromSourceAssignment(dataset, dataTable, validatedSourceAssignment);
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (err) {
    logger.error(err, `An error occurred trying to process the source assignments: ${err}`);

    if (err instanceof SourceAssignmentException) {
      next(new BadRequestException(err.message));
    } else {
      next(new UnknownException('errors.unknown_server_error'));
    }
  }
};

export const getFactTableDefinition = async (req: Request, res: Response) => {
  const dataset: Dataset = res.locals.dataset;
  const factTableDto: FactTableColumnDto[] =
    dataset.factTable?.map((col: FactTableColumn) => FactTableColumnDto.fromFactTableColumn(col)) || [];
  res.status(200);
  res.json(factTableDto);
};

export const getAllFilesForDataset = async (req: Request, res: Response) => {
  const dataset: Dataset = res.locals.dataset;
  const datasetFiles = collectFiles(dataset);
  const zip = new JSZip();
  try {
    await addDirectoryToZip(zip, datasetFiles, dataset.id, req.fileService);
  } catch (err) {
    logger.error(err, `Failed to get files from datalake for dataset ${dataset.id}`);
    res.status(500);
    res.end();
    return;
  }
  zip.file('dataset.json', JSON.stringify(DatasetDTO.fromDataset(dataset)));

  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': `application/zip`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Disposition': `attachment; filename=${dataset.id}.zip`
  });
  zip
    .generateNodeStream({ type: 'nodebuffer', streamFiles: true })
    .pipe(res)
    .on('finish', () => {
      res.end();
    });
};

export const listAllFilesInDataset = async (req: Request, res: Response) => {
  const dataset: Dataset = res.locals.dataset;
  const datasetFiles = collectFiles(dataset);
  const files = Array.from(datasetFiles.values());
  res.json(files);
};
