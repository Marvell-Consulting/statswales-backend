import { NextFunction, Request, Response } from 'express';
import { last, sortBy } from 'lodash';
import { t } from 'i18next';
import JSZip from 'jszip';

import { User } from '../entities/user/user';
import {
  DatasetRepository,
  withDeveloperPreview,
  withDimensions,
  withDraftAndMeasure,
  withDraftAndMetadata,
  withDraftAndProviders,
  withDraftAndTopics,
  withDraftForCube,
  withFactTable,
  withLatestRevision,
  withStandardPreview
} from '../repositories/dataset';
import { Locale } from '../enums/locale';
import { logger } from '../utils/logger';
import { UnknownException } from '../exceptions/unknown.exception';
import { DatasetDTO } from '../dtos/dataset-dto';
import { userGroupIdValidator, hasError, titleValidator } from '../validators';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
import { createAllCubeFiles } from '../services/cube-handler';
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

import { getPostgresCubePreview } from '../services/cube-handler';
import { factTableValidatorFromSource } from '../services/fact-table-validator';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { addDirectoryToZip, collectFiles } from '../utils/dataset-controller-utils';
import { NotAllowedException } from '../exceptions/not-allowed.exception';
import { GroupRole } from '../enums/group-role';
import { DatasetInclude } from '../enums/dataset-include';
import { EventLogDTO } from '../dtos/event-log-dto';
import { EventLog } from '../entities/event-log';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { cleanupTmpFile, uploadAvScan } from '../services/virus-scanner';
import { TempFile } from '../interfaces/temp-file';
import { DEFAULT_PAGE_SIZE } from '../utils/page-defaults';
import { PublisherDTO } from '../dtos/publisher-dto';
import { UserGroupRepository } from '../repositories/user-group';
import { dbManager } from '../db/database-manager';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { TaskAction } from '../enums/task-action';
import { TaskService } from '../services/task';

export const listUserDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const user = req.user as User;
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 20;
    const search = (req.query.search as string | undefined)?.trim().slice(0, 100);
    const results = await DatasetRepository.listForUser(user, lang, page, limit, search);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch dataset list');
    next(new UnknownException());
  }
};

export const listAllDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 20;
    const search = req.query.search as string | undefined;
    const results = await DatasetRepository.listAll(lang, page, limit, search);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch dataset list');
    next(new UnknownException());
  }
};

export const getDatasetById = async (req: Request, res: Response): Promise<void> => {
  const datasetId: string = res.locals.datasetId;
  const hydrate = req.query.hydrate as DatasetInclude;

  let dataset: Dataset = res.locals.dataset; // plain dataset without any relations
  let datasetDTO: DatasetDTO;

  switch (hydrate) {
    case DatasetInclude.Preview:
      dataset = await DatasetRepository.getById(datasetId, withStandardPreview);
      datasetDTO = DatasetDTO.fromDataset(dataset);

      if (dataset.userGroupId) {
        const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
        datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, req.language as Locale);
      }

      res.json(datasetDTO);
      return;

    case DatasetInclude.Developer:
      dataset = await DatasetRepository.getById(datasetId, withDeveloperPreview);
      break;

    case DatasetInclude.LatestRevision:
      dataset = await DatasetRepository.getById(datasetId, withLatestRevision);
      break;

    case DatasetInclude.Data:
      dataset = await DatasetRepository.getById(datasetId, withDraftForCube);
      break;

    case DatasetInclude.Dimensions:
      dataset = await DatasetRepository.getById(datasetId, withDimensions);
      break;

    case DatasetInclude.Measure:
      dataset = await DatasetRepository.getById(datasetId, withDraftAndMeasure);
      break;

    case DatasetInclude.Meta:
      dataset = await DatasetRepository.getById(datasetId, withDraftAndMetadata);
      datasetDTO = DatasetDTO.fromDataset(dataset);

      if (dataset.userGroupId) {
        const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
        datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, req.language as Locale);
      }

      res.json(datasetDTO);
      return;

    case DatasetInclude.Overview:
      dataset = await req.datasetService.getDatasetOverview(datasetId);
      break;
  }

  res.json(DatasetDTO.fromDataset(dataset));
};

export const deleteDraftDatasetById = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset: Dataset = res.locals.dataset;
  if (dataset.publishedRevision) {
    next(new NotAllowedException('Dataset is already published, cannot delete'));
    return;
  }
  await req.fileService.deleteDirectory(req.params.dataset_id);

  const datasetWithDraftAndDimensions = await DatasetRepository.getById(dataset.id, {
    dimensions: { lookupTable: true },
    draftRevision: { dataTable: true, previousRevision: true }
  });
  const draft = datasetWithDraftAndDimensions.draftRevision;

  if (draft) {
    const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
    try {
      await cubeDB.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE', draft.id));
      if (draft.dataTable?.id) {
        await cubeDB.query(pgformat('DROP TABLE IF EXISTS data_tables.%I;', draft.dataTable?.id));
      }
      for (const dim of datasetWithDraftAndDimensions.dimensions) {
        if (dim.lookupTable) {
          await cubeDB.query(pgformat('DROP TABLE IF EXISTS lookup_tables.%I;', dim.lookupTable?.id));
        }
      }
    } catch (err) {
      logger.warn(err, `Failed to clean up cube database when deleting draft revision ${draft.id}`);
    } finally {
      await cubeDB.release();
    }
  }

  await DatasetRepository.deleteById(res.locals.datasetId);
  res.status(202);
  res.end();
};

export const createDataset = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const uploadDataTable = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset: Dataset = res.locals.dataset;
  let tmpFile: TempFile;

  try {
    tmpFile = await uploadAvScan(req);
  } catch (err) {
    logger.error(err, 'There was a problem uploading the data table file');
    next(err);
    return;
  }

  logger.info(`Processing data table upload for dataset ${dataset.id}...`);
  logger.debug(`File received: ${tmpFile.originalname}, mimetype: ${tmpFile.mimetype}, size: ${tmpFile.size} bytes`);

  try {
    const updatedDataset = await req.datasetService.updateFactTable(dataset.id, tmpFile);
    const dto = DatasetDTO.fromDataset(updatedDataset);
    res.status(201).json(dto);
    return;
  } catch (err) {
    logger.error(err, 'Failed to update the fact table');
    const lang = req.language as Locale;
    const error: ViewErrDTO = {
      status: 500,
      dataset_id: dataset.id,
      errors: [
        {
          field: 'csv',
          message: { key: 'errors.unknown_error', params: {} },
          user_message: [{ lang, message: t('errors.unknown_error', { lng: lang }) }]
        }
      ]
    };
    res.status(500).json(error);
    return;
  } finally {
    cleanupTmpFile(tmpFile);
  }
};

export const cubePreview = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = dataset.draftRevision ?? last(sortBy(dataset?.revisions, 'revisionIndex'));
  const lang = req.language.split('-')[0];

  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }

  const start = performance.now();
  const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  const cubePreview = await getPostgresCubePreview(
    latestRevision,
    lang,
    dataset,
    page_number,
    page_size,
    sortByQuery,
    filterQuery
  );
  const end = performance.now();
  const time = Math.round(end - start);
  logger.info(`Generating preview of cube took ${time}ms`);
  if ((cubePreview as ViewErrDTO).errors) {
    res.status(500);
  }
  res.json(cubePreview);
};

export const updateMetadata = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const getTasklist = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const datasetId = res.locals.datasetId;
  try {
    const tasklistState = await req.datasetService.getTasklistState(datasetId, req.language as Locale);
    res.json(tasklistState);
  } catch (err) {
    logger.error(err, `There was a problem fetching the tasklist for dataset ${datasetId}`);
    next(new UnknownException('errors.tasklist_error'));
  }
};

export const getDataProviders = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftAndProviders);
    const providers = dataset.draftRevision?.revisionProviders?.map((provider: RevisionProvider) =>
      RevisionProviderDTO.fromRevisionProvider(provider)
    );
    res.json(providers);
  } catch (err) {
    next(err);
  }
};

export const addDataProvider = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const updateDataProviders = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const getTopics = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftAndTopics);
    const revisionTopics = dataset?.draftRevision?.revisionTopics || [];
    const topics = revisionTopics.map((revTopic: RevisionTopic) => TopicDTO.fromTopic(revTopic.topic));
    res.json(topics);
  } catch (err) {
    next(err);
  }
};

export const updateTopics = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const updateSources = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
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
  try {
    await factTableValidatorFromSource(dataset, validatedSourceAssignment);
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
    await createDimensionsFromSourceAssignment(dataset, dataTable, validatedSourceAssignment);
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    await createAllCubeFiles(updatedDataset.id, revision.id);
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

export const getFactTableDefinition = async (req: Request, res: Response): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withFactTable);
  const factTableDto: FactTableColumnDto[] =
    dataset.factTable?.map((col: FactTableColumn) => FactTableColumnDto.fromFactTableColumn(col)) || [];
  res.status(200);
  res.json(factTableDto);
};

export const getAllFilesForDataset = async (req: Request, res: Response): Promise<void> => {
  const datasetId: string = res.locals.datasetId;
  const zip = new JSZip();

  try {
    const dataset = await DatasetRepository.getById(datasetId, withDraftForCube);
    const datasetFiles = collectFiles(dataset);
    await addDirectoryToZip(zip, datasetFiles, dataset.id, req.fileService);
    zip.file('dataset.json', JSON.stringify(DatasetDTO.fromDataset(dataset)));
  } catch (err) {
    logger.error(err, `Failed to get files from datalake for dataset ${datasetId}`);
    res.status(500);
    res.end();
    return;
  }

  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': `application/zip`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Disposition': `attachment; filename=${datasetId}.zip`
  });
  zip
    .generateNodeStream({ type: 'nodebuffer', streamFiles: true })
    .pipe(res)
    .on('finish', () => {
      res.end();
    });
};

export const listAllFilesInDataset = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const datasetId: string = res.locals.datasetId;

  try {
    const dataset = await DatasetRepository.getById(datasetId, withDraftForCube);
    const datasetFiles = collectFiles(dataset);
    const files = Array.from(datasetFiles.values());
    res.json(files);
  } catch (err) {
    logger.error(err, `Failed to list all files for ${datasetId}`);
    next(new UnknownException('errors.list_files_error'));
  }
};

export const updateDatasetGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset: Dataset = res.locals.dataset;

  const validUserGroupIds = req.user?.groupRoles
    .filter((gr) => gr.roles.includes(GroupRole.Approver))
    .map((gr) => gr.groupId) as string[];

  const groupIdError = await hasError(userGroupIdValidator(validUserGroupIds), req);

  if (groupIdError) {
    next(new BadRequestException('errors.user_group_id.invalid'));
    return;
  }

  try {
    const { user_group_id } = req.body;
    const updatedDataset = await req.datasetService.updateDatasetGroup(dataset.id, user_group_id);
    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (err) {
    logger.error(err, `Failed to update dataset group`);
    next(new UnknownException());
  }
};

export const getHistory = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const datasetId = res.locals.datasetId;

  try {
    const history = await req.datasetService.getHistory(datasetId);
    const eventLogDTOs = history.map((event: EventLog) => EventLogDTO.fromEventLog(event));

    res.json(eventLogDTOs);
  } catch (err) {
    logger.error(err, `There was a problem fetching the history for dataset ${datasetId}`);
    next(new UnknownException('errors.dataset_history'));
  }
};

export const datasetActionRequest = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const action = req.params.action as TaskAction;

  if (!Object.values(TaskAction).includes(action)) {
    next();
    return;
  }

  const datasetId = res.locals.datasetId;
  const user = req.user as User;
  const { reason } = req.body;
  const taskService = new TaskService();

  switch (action) {
    case TaskAction.Publish:
      throw new BadRequestException('Publish request is handled via the revision endpoint');
    case TaskAction.Unpublish:
      await taskService.requestUnpublish(datasetId, user, reason);
      break;
    case TaskAction.Archive:
      await taskService.requestArchive(datasetId, user, reason);
      break;
    case TaskAction.Unarchive:
      await taskService.requestUnarchive(datasetId, user, reason);
      break;
  }

  res.status(204).end();
};
