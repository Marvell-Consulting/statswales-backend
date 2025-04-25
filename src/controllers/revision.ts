import fs from 'node:fs';
import { Readable } from 'node:stream';
import { performance } from 'node:perf_hooks';

import { NextFunction, Request, Response } from 'express';
import tmp from 'tmp';
import { t } from 'i18next';
import { isBefore, isValid } from 'date-fns';

import { User } from '../entities/user/user';
import { DataTableDto } from '../dtos/data-table-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { ViewErrDTO } from '../dtos/view-dto';
import { Revision } from '../entities/dataset/revision';
import { logger } from '../utils/logger';
import { DataTable } from '../entities/dataset/data-table';
import { Locale } from '../enums/locale';
import { DatasetRepository } from '../repositories/dataset';
import { DatasetDTO } from '../dtos/dataset-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { RevisionDTO } from '../dtos/revision-dto';
import { RevisionRepository } from '../repositories/revision';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import {
  cleanUpCube,
  createBaseCube,
  createBaseCubeFromProtoCube,
  createDateDimension,
  createLookupTableDimension,
  loadCorrectReferenceDataIntoReferenceDataTable,
  loadReferenceDataIntoCube,
  makeCubeSafeString,
  updateFactTableValidator
} from '../services/cube-handler';
import { DEFAULT_PAGE_SIZE, getCSVPreview, validateAndUploadCSV } from '../services/csv-processor';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { DataTableAction } from '../enums/data-table-action';
import { ColumnMatch } from '../interfaces/column-match';
import { DimensionType } from '../enums/dimension-type';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { DimensionUpdateTask } from '../interfaces/revision-task';
import { duckdb } from '../services/duckdb';
import { Dataset } from '../entities/dataset/dataset';

import { getCubePreview, outputCube } from './cube-controller';
import { FileValidationException } from '../exceptions/validation-exception';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { checkForReferenceErrors } from '../services/lookup-table-handler';
import { validateUpdatedDateDimension } from '../services/dimension-processor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { NotAllowedException } from '../exceptions/not-allowed.exception';

export const getDataTable = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const dataTable = await DataTable.findOneOrFail({
      where: { id: req.params.id },
      relations: { dataTableDescriptions: true, revision: true }
    });
    const dto = DataTableDto.fromDataTable(dataTable);
    res.json(dto);
  } catch (_err) {
    next(new UnknownException());
  }
};

export const deleteDraftRevision = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;

  if (revision.revisionIndex !== 0) {
    next(new NotAllowedException('Revision is not a draft, cannot delete'));
    return;
  }

  try {
    await req.datasetService.deleteDraftRevision(dataset.id, revision.id);
  } catch (err) {
    next(err);
    return;
  }

  res.status(202);
  res.end();
};

export const getDataTablePreview = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;

  const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

  if (!revision.dataTable) {
    next(new NotFoundException('errors.no_data_table'));
    return;
  }

  const processedCSV = await getCSVPreview(dataset, revision, revision.dataTable, page_number, page_size);

  if ((processedCSV as ViewErrDTO).errors) {
    const processErr = processedCSV as ViewErrDTO;
    res.status(processErr.status);
  }

  res.json(processedCSV);
};

export const getRevisionPreview = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = res.locals.dataset;
  const revision = res.locals.revision;
  const lang = req.language.split('-')[0];
  const start = performance.now();

  const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('protocube')) {
    logger.debug('Loading cube from file store for preview');
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    try {
      const cubeBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
      fs.writeFileSync(cubeFile, cubeBuffer);
    } catch (err) {
      logger.error('Something went wrong trying to download file from data lake');
      throw err;
    }
  } else if (revision.onlineCubeFilename?.includes('protocube')) {
    logger.debug('Loading protocube from file store for preview');
    const buffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, buffer);
    await createBaseCubeFromProtoCube(dataset.id, revision.id, cubeFile);
  } else {
    logger.debug('Creating fresh cube for preview... This could take a few seconds');
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (error) {
      logger.error(`Something went wrong trying to create the cube with the error: ${error}`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const cubePreview = await getCubePreview(cubeFile, lang, dataset, page_number, page_size);
  const end = performance.now();
  const time = Math.round(end - start);
  logger.info(`Cube revision preview took ${time}ms`);
  await cleanUpCube(cubeFile);
  if ((cubePreview as ViewErrDTO).errors) {
    const processErr = cubePreview as ViewErrDTO;
    res.status(processErr.status);
  }

  res.json(cubePreview);
};

export const confirmFactTable = async (req: Request, res: Response) => {
  const revision = res.locals.revision;
  const dto = DataTableDto.fromDataTable(revision.dataTable);
  res.json(dto);
};

export const downloadRawFactTable = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  logger.info('User requested to down files...');
  let readable: Readable;
  logger.debug(`${JSON.stringify(revision)}`);
  if (!revision.dataTable) {
    logger.error("Revision doesn't have a data table, can't download file");
    next(new NotFoundException('errors.revision_id_invalid'));
    return;
  }

  try {
    readable = await req.fileService.loadStream(revision.dataTable.filename, dataset.id);
  } catch (_err) {
    res.status(500);
    res.json({
      status: 500,
      errors: [
        {
          field: 'csv',
          message: [
            {
              lang: Locale.English,
              message: t('errors.download_from_filestore', { lng: Locale.English })
            },
            {
              lang: Locale.Welsh,
              message: t('errors.download_from_filestore', { lng: Locale.Welsh })
            }
          ],
          tag: { name: 'errors.download_from_filestore', params: {} }
        }
      ],
      dataset_id: dataset.id
    });
    return;
  }
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': `${revision.dataTable.mimeType}`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Disposition': `attachment; filename=${revision.dataTable.originalFilename}`
  });
  readable.pipe(res);

  // Handle errors in the file stream
  readable.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  readable.on('end', () => {
    logger.debug('File stream ended');
  });
};

export const getRevisionInfo = async (req: Request, res: Response) => {
  const revision = res.locals.revision;
  res.json(RevisionDTO.fromRevision(revision));
};

async function attachUpdateDataTableToRevision(
  dataset: Dataset,
  revision: Revision,
  dataTable: DataTable,
  updateAction: DataTableAction,
  columnMatcher?: ColumnMatch[]
) {
  logger.debug('Attaching update data table to revision and validating cube');
  const start = performance.now();

  // Validate all the columns against the fact table
  if (columnMatcher) {
    const matchedColumns: string[] = [];
    for (const col of columnMatcher) {
      const factTableCol: FactTableColumn | undefined = dataset.factTable?.find(
        (factTableCol: FactTableColumn) =>
          makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.fact_table_column_name)
      );
      const dataTableCol = dataTable.dataTableDescriptions.find(
        (dataTableCol: DataTableDescription) =>
          makeCubeSafeString(dataTableCol.columnName) === makeCubeSafeString(col.data_table_column_name)
      );
      if (factTableCol && dataTableCol) {
        matchedColumns.push(factTableCol.columnName);
        dataTableCol.factTableColumn = factTableCol.columnName;
      }
    }
    if (matchedColumns.length !== dataset.factTable?.length) {
      logger.error(`Could not match all columns to the fact table.`);
      throw new UnknownException('errors.failed_to_match_columns');
    }
  } else {
    // validate columns
    const matchedColumns: string[] = [];
    const unmatchedColumns: string[] = [];
    for (const col of dataTable.dataTableDescriptions) {
      const factTableCol: FactTableColumn | undefined = dataset.factTable?.find(
        (factTableCol: FactTableColumn) =>
          makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.columnName)
      );
      if (factTableCol) {
        matchedColumns.push(factTableCol.columnName);
        col.factTableColumn = factTableCol.columnName;
      } else {
        unmatchedColumns.push(col.columnName);
      }
    }

    if (matchedColumns.length !== dataset.factTable?.length) {
      logger.error(
        `Could not match all columns to the fact table.  The following columns were not matched: ${unmatchedColumns.join(', ')}`
      );
      const end = performance.now();
      const time = Math.round(end - start);
      logger.info(`Cube update validation took ${time}ms`);
      throw new UnknownException('errors.failed_to_match_columns');
    }
  }

  logger.debug(`Setting the update action to: ${updateAction}`);
  dataTable.action = updateAction;
  revision.dataTable = dataTable;
  const quack = await duckdb();

  try {
    await updateFactTableValidator(quack, dataset, revision);
  } catch (err) {
    const error = err as CubeValidationException;
    if (error.type === CubeValidationType.DuplicateFact) {
      error.type = CubeValidationType.UnknownDuplicateFact;
    }
    logger.debug('Closing DuckDB instance');
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube update validation took ${time}ms`);
    await quack.close();
    throw error;
  }

  const dimensionUpdateTasks: DimensionUpdateTask[] = [];
  if (dataset.dimensions.find((dimension) => dimension.type === DimensionType.ReferenceData)) {
    await loadReferenceDataIntoCube(quack);
  }
  for (const dimension of dataset.dimensions) {
    const factTableColumn = dataset.factTable.find(
      (factTableColumn) =>
        factTableColumn.columnName === dimension.id && factTableColumn.columnType === FactTableColumnType.Dimension
    );
    if (!factTableColumn) {
      logger.error(`Could not find fact table column for dimension ${dimension.id}`);
      throw new BadRequestException('errors.data_table_validation_error');
    }
    try {
      switch (dimension.type) {
        case DimensionType.LookupTable:
          logger.debug(`Validating lookup table dimension: ${dimension.id}`);
          await createLookupTableDimension(quack, dataset, dimension);
          await checkForReferenceErrors(quack, dataset, dimension, factTableColumn);
          break;
        case DimensionType.ReferenceData:
          logger.debug(`Validating reference data dimension: ${dimension.id}`);
          await loadCorrectReferenceDataIntoReferenceDataTable(quack, dimension);
          break;
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          logger.debug(`Validating time dimension: ${dimension.id}`);
          await createDateDimension(quack, dimension.extractor, factTableColumn);
          await validateUpdatedDateDimension(quack, dataset, dimension, factTableColumn);
      }
    } catch (error) {
      logger.warn(`An error occurred validating dimension ${dimension.id}: ${error}`);
      const err = error as CubeValidationException;
      if (err.type === CubeValidationType.DimensionNonMatchedRows) {
        dimensionUpdateTasks.push({
          id: dimension.id,
          lookupTableUpdated: false
        });
      } else {
        logger.debug('Closing DuckDB instance');
        const end = performance.now();
        const time = Math.round(end - start);
        logger.info(`Cube update validation took ${time}ms`);
        await quack.close();
        logger.error(`An error occurred trying to validate the file with the following error: ${err}`);
        throw new BadRequestException('errors.data_table_validation_error');
      }
    }
  }

  // TODO Validate measure.  This requires a rewrite of how measures are created and stored

  revision.tasks = { dimensions: dimensionUpdateTasks };

  logger.debug('Closing DuckDB instance');
  await quack.close();
  await revision.save();
  const end = performance.now();
  const time = Math.round(end - start);
  logger.info(`Cube update validation took ${time}ms`);

  dataTable.revision = revision;
  await dataTable.save();
}

export const updateDataTable = async (req: Request, res: Response, next: NextFunction) => {
  const dataset: Dataset = res.locals.dataset;
  const revision: Revision = res.locals.revision;

  logger.debug(`Updating data table for revision ${revision.id}`);

  if (!req.file) {
    next(new BadRequestException('errors.upload.no_csv'));
    return;
  }

  if (revision.dataTable) {
    logger.debug(`Revision ${revision.id} already has a data table ${revision.dataTable.id}, removing it`);
    try {
      await req.fileService.delete(revision.dataTable.filename, dataset.id);
    } catch (err) {
      logger.warn(err, `Failed to delete data table file ${revision.dataTable.filename} from data lake`);
    }
    await DataTable.getRepository().remove(revision.dataTable);
  }
  let dataTable: DataTable;
  try {
    const { mimetype, originalname } = req.file;
    const uploadResult = await validateAndUploadCSV(req.file.buffer, mimetype, originalname, dataset.id);
    dataTable = uploadResult.dataTable;
  } catch (err) {
    const error = err as FileValidationException;
    logger.error(error, `An error occurred trying to upload the file`);
    if (error.status === 500) {
      return next(new UnknownException(error.errorTag));
    } else {
      const error = err as FileValidationException;
      return next(new BadRequestException(error.errorTag));
    }
  }

  try {
    if (revision.revisionIndex === 1) {
      logger.debug('Attaching data table to first revision');
      await RevisionRepository.save({ ...revision, dataTable });
    } else {
      const columnMatcher = req.body.column_matching
        ? (JSON.parse(req.body.column_matching) as ColumnMatch[])
        : undefined;
      const updateAction = req.body.update_action ? (req.body.update_action as DataTableAction) : DataTableAction.Add;
      await attachUpdateDataTableToRevision(dataset, revision, dataTable, updateAction, columnMatcher);
    }
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (err) {
    logger.error(err, `An error occurred trying to update the dataset`);
    const error = err as FactTableValidationException;
    if (error.type) {
      res.status(error.status);
      const viewErr: ViewErrDTO = {
        status: error.status,
        dataset_id: dataset.id,
        errors: [
          {
            field: 'csv',
            message: {
              key: `errors.fact_table_validation.${error.type}`,
              params: {}
            },
            user_message: [
              {
                lang: req.language,
                message: t(`errors.fact_table_validation.${error.type}`, { lng: req.language })
              }
            ]
          }
        ]
      };
      res.json(viewErr);
      return;
    }
    logger.error(err, `An unknown error occurred trying to update the dataset`);
    next(new UnknownException('errors.fact_table_validation.unknown_error'));
  }
};

export const removeFactTableFromRevision = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;

  if (!revision.dataTable) {
    logger.error("Revision doesn't have a data table, can't remove file");
    next(new NotFoundException('errors.revision_id_invalid'));
    return;
  }

  try {
    logger.warn('User has requested to remove a fact table from the filestore');
    await req.fileService.delete(revision.dataTable.filename, dataset.id);
    if (dataset.revisions.length === 1) {
      for (const factTableCol of dataset.factTable) {
        await factTableCol.remove();
      }
    }
    await revision.dataTable.remove();
    const updatedDataset = await DatasetRepository.getById(dataset.id);
    const dto = DatasetDTO.fromDataset(updatedDataset);
    res.json(dto);
  } catch (err) {
    logger.error(`An error occurred trying to remove the file with the following error: ${err}`);
    next(new UnknownException('errors.remove_file'));
  }
};

export const updateRevisionPublicationDate = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = res.locals.dataset;
  const revision = res.locals.revision;

  if (revision.approvedAt) {
    next(new BadRequestException('errors.publish_at.revision_already_approved'));
    return;
  }

  try {
    const publishAt = req.body.publish_at;

    if (!publishAt || !isValid(new Date(publishAt))) {
      next(new BadRequestException('errors.publish_at.invalid'));
      return;
    }

    if (isBefore(publishAt, new Date())) {
      next(new BadRequestException('errors.publish_at.in_past'));
      return;
    }

    await RevisionRepository.updatePublishDate(revision, publishAt);
    const updatedDataset = await DatasetRepository.getById(dataset.id, {});

    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (_err) {
    next(new UnknownException());
  }
};

export const approveForPublication = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  const user = req.user as User;

  try {
    if (revision.approvedAt) {
      throw new BadRequestException('errors.approve.revision_already_approved');
    }

    const tasklistState = await req.datasetService.getTasklistState(dataset.id, req.language as Locale);

    if (!tasklistState.canPublish) {
      logger.error('Dataset is not ready for publication, check tasklist state');
      throw new BadRequestException('errors.approve.not_ready');
    }

    const approvedDataset = await req.datasetService.approveForPublication(dataset.id, revision.id, user);

    res.status(201);
    res.json(DatasetDTO.fromDataset(approvedDataset));
  } catch (err: unknown) {
    next(err);
  }
};

export const withdrawFromPublication = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { dataset, revision } = res.locals;

    if (!revision.publishAt || !revision.approvedAt) {
      throw new BadRequestException('errors.withdraw.not_scheduled');
    }

    if (isBefore(revision.publishAt, new Date())) {
      throw new BadRequestException('errors.withdraw.already_published');
    }

    const withdrawnDataset = await req.datasetService.withdrawFromPublication(dataset.id, revision.id);
    res.status(201);
    res.json(DatasetDTO.fromDataset(withdrawnDataset));
  } catch (err: unknown) {
    logger.error(err, 'could not withdraw publication');
    next(err);
  }
};

export const downloadRevisionCubeFile = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
  } else if (revision.onlineCubeFilename && revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
    cubeFile = await createBaseCubeFromProtoCube(dataset.id, revision, cubeFile);
  } else {
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (err) {
      logger.error(err, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const fileBuffer = Buffer.from(fs.readFileSync(cubeFile));
  logger.info(`Sending original cube file (size: ${fileBuffer.length}) from: ${cubeFile}`);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/octet-stream',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': fileBuffer.length
  });
  res.end(fileBuffer);
  await cleanUpCube(cubeFile);
};

export const downloadRevisionCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  const lang = req.language.split('-')[0];
  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
  } else if (revision.onlineCubeFilename && revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
    cubeFile = await createBaseCubeFromProtoCube(dataset.id, revision, cubeFile);
  } else {
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (err) {
      logger.error(err, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Json);
  await cleanUpCube(cubeFile);
  const downloadStream = fs.createReadStream(downloadFile);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  res.writeHead(200, { 'Content-Type': '\tapplication/json' });
  downloadStream.pipe(res);

  // Handle errors in the file stream
  downloadStream.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    fs.unlinkSync(downloadFile);
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  downloadStream.on('end', () => {
    fs.unlinkSync(downloadFile);
    logger.debug('File stream ended');
  });
};

export const downloadRevisionCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  const lang = req.language.split('-')[0];
  if (!revision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
  } else if (revision.onlineCubeFilename && revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
    cubeFile = await createBaseCubeFromProtoCube(dataset.id, revision, cubeFile);
  } else {
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (err) {
      logger.error(err, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Csv);
  await cleanUpCube(cubeFile);
  const downloadStream = fs.createReadStream(downloadFile);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  res.writeHead(200, { 'Content-Type': '\ttext/csv' });
  downloadStream.pipe(res);

  // Handle errors in the file stream
  downloadStream.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    fs.unlinkSync(downloadFile);
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  downloadStream.on('end', () => {
    fs.unlinkSync(downloadFile);
    logger.debug('File stream ended');
  });
};

export const downloadRevisionCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  const lang = req.language.split('-')[0];
  if (!revision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
  } else if (revision.onlineCubeFilename && revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
    cubeFile = await createBaseCubeFromProtoCube(dataset.id, revision, cubeFile);
  } else {
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (err) {
      logger.error(err, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Parquet);
  await cleanUpCube(cubeFile);
  const downloadStream = fs.createReadStream(downloadFile);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  res.writeHead(200, { 'Content-Type': '\tapplication/vnd.apache.parquet' });
  downloadStream.pipe(res);

  // Handle errors in the file stream
  downloadStream.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    fs.unlinkSync(downloadFile);
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  downloadStream.on('end', () => {
    fs.unlinkSync(downloadFile);
    logger.debug('File stream ended');
  });
};

export const downloadRevisionCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
  const { dataset, revision } = res.locals;
  const lang = req.language.split('-')[0];
  if (!revision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  let cubeFile: string;
  if (revision.onlineCubeFilename && !revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
  } else if (revision.onlineCubeFilename && revision.onlineCubeFilename.includes('proto')) {
    const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
    cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
    fs.writeFileSync(cubeFile, fileBuffer);
    cubeFile = await createBaseCubeFromProtoCube(dataset.id, revision, cubeFile);
  } else {
    try {
      cubeFile = await createBaseCube(dataset.id, revision.id);
    } catch (err) {
      logger.error(err, `Something went wrong trying to create the cube`);
      next(new UnknownException('errors.cube_builder.cube_build_failed'));
      return;
    }
  }
  const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Excel);
  logger.info(`Cube file located at: ${cubeFile}`);
  await cleanUpCube(cubeFile);
  const downloadStream = fs.createReadStream(downloadFile);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  res.writeHead(200, { 'Content-Type': '\tapplication/vnd.ms-excel' });
  downloadStream.pipe(res);

  // Handle errors in the file stream
  downloadStream.on('error', (err) => {
    logger.error('File stream error:', err);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    fs.unlinkSync(downloadFile);
    res.end('Server Error');
  });

  // Optionally listen for the end of the stream
  downloadStream.on('end', () => {
    fs.unlinkSync(downloadFile);
    logger.debug('File stream ended');
  });
};

export const createNewRevision = async (req: Request, res: Response, next: NextFunction) => {
  const user = req.user as User;

  try {
    const dataset = await req.datasetService.createRevision(res.locals.datasetId, user);
    res.status(201);
    res.json(RevisionDTO.fromRevision(dataset.draftRevision!));
  } catch (err) {
    next(err);
  }
};
