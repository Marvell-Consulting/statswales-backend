import fs from 'node:fs';
import { Readable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { pipeline } from 'node:stream/promises';

import { NextFunction, Request, Response } from 'express';
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
  createAllCubeFiles,
  createDateDimension,
  createLookupTableDimension,
  loadCorrectReferenceDataIntoReferenceDataTable,
  loadReferenceDataIntoCube,
  makeCubeSafeString,
  updateFactTableValidator
} from '../services/cube-handler';
import {
  DEFAULT_PAGE_SIZE,
  extractTableInformation,
  getCSVPreview,
  validateAndUpload
} from '../services/csv-processor';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { DataTableAction } from '../enums/data-table-action';
import { ColumnMatch } from '../interfaces/column-match';
import { DimensionType } from '../enums/dimension-type';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { DimensionUpdateTask } from '../interfaces/revision-task';
import { duckdb } from '../services/duckdb';
import { FileValidationException } from '../exceptions/validation-exception';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { checkForReferenceErrors } from '../services/lookup-table-handler';
import { validateUpdatedDateDimension } from '../services/dimension-processor';
import { CubeValidationType } from '../enums/cube-validation-type';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { NotAllowedException } from '../exceptions/not-allowed.exception';

import { getPostgresCubePreview, outputCube } from './cube-controller';
import { Dataset } from '../entities/dataset/dataset';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { FindOptionsRelations } from 'typeorm';
import {
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView,
  getFilters
} from '../services/consumer-view';
import { asyncTmpName } from '../utils/async-tmp';
import { FileType } from '../enums/file-type';
import { cleanupTmpFile, uploadAvScan } from '../services/virus-scanner';
import { TempFile } from '../interfaces/temp-file';

export const getDataTable = async (req: Request, res: Response, next: NextFunction) => {
  const revision: Revision = res.locals.revision;

  if (!revision.dataTableId) {
    throw new NotFoundException('errors.revision.no_data_table');
  }

  try {
    const dataTable = await DataTable.findOneOrFail({
      where: { id: revision.dataTableId },
      relations: { dataTableDescriptions: true, revision: true }
    });
    const dto = DataTableDto.fromDataTable(dataTable);
    res.json(dto);
  } catch (_err) {
    logger.error(_err, `There was a problem fetching the data table for revision ${revision.id}`);
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
  const datasetId: string = res.locals.datasetId;
  const revision = res.locals.revision;

  const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

  if (!revision.dataTable) {
    next(new NotFoundException('errors.no_data_table'));
    return;
  }

  const processedCSV = await getCSVPreview(datasetId, revision.dataTable, page_number, page_size);

  if ((processedCSV as ViewErrDTO).errors) {
    const processErr = processedCSV as ViewErrDTO;
    res.status(processErr.status);
  }

  res.json(processedCSV);
};

export const getRevisionPreview = async (req: Request, res: Response) => {
  const dataset: Dataset = res.locals.dataset;
  const revision = res.locals.revision;
  const lang = req.language.split('-')[0];
  const start = performance.now();

  const page_number: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const page_size: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  try {
    const end = performance.now();
    const cubePreview = await getPostgresCubePreview(
      revision,
      lang,
      dataset,
      page_number,
      page_size,
      sortByQuery,
      filterQuery
    );
    const time = Math.round(end - start);
    logger.info(`Cube revision preview took ${time}ms`);

    if ((cubePreview as ViewErrDTO).errors) {
      const processErr = cubePreview as ViewErrDTO;
      res.status(processErr.status);
    }

    res.json(cubePreview);
  } catch (err) {
    logger.error(err, `An error occurred trying to get the cube preview`);
  }
};

export const getRevisionPreviewFilters = async (req: Request, res: Response) => {
  const revision: Revision = res.locals.revision;
  const lang = req.language.length < 5 ? `${req.language}-gb` : req.language.toLowerCase();
  if (!revision) {
    throw new NotFoundException('errors.no_revision');
  }

  const filters = await getFilters(revision, lang);
  res.json(filters);
};

export const confirmFactTable = async (req: Request, res: Response) => {
  const revision = res.locals.revision;
  const dto = DataTableDto.fromDataTable(revision.dataTable);
  res.json(dto);
};

export const downloadRawFactTable = async (req: Request, res: Response, next: NextFunction) => {
  const datasetId = res.locals.datasetId;
  const revision = res.locals.revision;
  logger.info('User requested to down files...');
  let readable: Readable;
  // logger.debug(`${JSON.stringify(revision)}`);

  if (!revision.dataTable) {
    logger.error("Revision doesn't have a data table, can't download file");
    next(new NotFoundException('errors.revision_id_invalid'));
    return;
  }

  try {
    readable = await req.fileService.loadStream(revision.dataTable.filename, datasetId);
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
      dataset_id: datasetId
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
  datasetId: string,
  revision: Revision,
  dataTable: DataTable,
  updateAction: DataTableAction,
  columnMatcher?: ColumnMatch[]
) {
  logger.debug('Attaching update data table to revision and validating cube');
  const start = performance.now();

  const dataset = await DatasetRepository.getById(datasetId, {
    factTable: true,
    measure: { measureTable: true, metadata: true },
    dimensions: { metadata: true, lookupTable: true },
    revisions: { dataTable: { dataTableDescriptions: true } }
  });

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
    await updateFactTableValidator(quack, dataset, revision, 'postgres');
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
        factTableColumn.columnName === dimension.factTableColumn &&
        factTableColumn.columnType === FactTableColumnType.Dimension
    );
    if (!factTableColumn) {
      logger.error(`Could not find fact table column for dimension ${dimension.id}`);
      throw new BadRequestException('errors.data_table_validation_error');
    }
    try {
      switch (dimension.type) {
        case DimensionType.LookupTable:
          logger.debug(`Validating lookup table dimension: ${dimension.id}`);
          await createLookupTableDimension(quack, dataset, dimension, factTableColumn);
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
  const datasetId: string = res.locals.datasetId;
  const revision: Revision = res.locals.revision;

  logger.debug(`Updating data table for revision ${revision.id}`);

  let tmpFile: TempFile;

  try {
    tmpFile = await uploadAvScan(req);
  } catch (err) {
    logger.error(err, 'There was a problem uploading the data table file');
    next(err);
    return;
  }

  if (revision.dataTable) {
    logger.debug(`Revision ${revision.id} already has a data table ${revision.dataTable.id}, removing it`);
    try {
      await req.fileService.delete(revision.dataTable.filename, datasetId);
    } catch (err) {
      logger.warn(err, `Failed to delete data table file ${revision.dataTable.filename} from data lake`);
    }
    await DataTable.getRepository().remove(revision.dataTable);
  }

  let dataTable: DataTable;
  try {
    dataTable = await validateAndUpload(tmpFile, datasetId, 'data_table');
  } catch (err) {
    const error = err as FileValidationException;
    logger.error(error, `An error occurred trying to upload the file`);

    if (error.status === 500) {
      return next(new UnknownException(error.errorTag));
    }

    return next(new BadRequestException(error.errorTag));
  } finally {
    cleanupTmpFile(tmpFile);
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
      await attachUpdateDataTableToRevision(datasetId, revision, dataTable, updateAction, columnMatcher);
    }
    const updatedDataset = await DatasetRepository.getById(datasetId);
    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (err) {
    logger.error(err, `An error occurred trying to update the dataset`);
    const error = err as FactTableValidationException;
    if (error.type) {
      res.status(error.status);
      const viewErr: ViewErrDTO = {
        status: error.status,
        dataset_id: datasetId,
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
  const datasetId = res.locals.datasetId;
  const revision = res.locals.revision;

  if (!revision.dataTable) {
    logger.error("Revision doesn't have a data table, can't remove file");
    next(new NotFoundException('errors.revision_id_invalid'));
    return;
  }

  try {
    logger.warn('User has requested to remove a fact table from the filestore');
    await req.fileService.delete(revision.dataTable.filename, datasetId);

    const dataset = await DatasetRepository.getById(datasetId, { factTable: true, revisions: true });

    if (dataset.revisions.length === 1 && dataset.factTable) {
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
  const datasetId: string = res.locals.datasetId;
  const revision: Revision = res.locals.revision;

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
    const updatedDataset = await DatasetRepository.getById(datasetId, {});

    res.status(201);
    res.json(DatasetDTO.fromDataset(updatedDataset));
  } catch (_err) {
    next(new UnknownException());
  }
};

export const submitForPublication = async (req: Request, res: Response, next: NextFunction) => {
  const datasetId: string = res.locals.datasetId;
  const revision: Revision = res.locals.revision;
  const user = req.user as User;

  try {
    if (revision.approvedAt) {
      throw new BadRequestException('errors.submit_for_publication.revision_already_approved');
    }

    const tasklistState = await req.datasetService.getTasklistState(datasetId, req.language as Locale);

    if (!tasklistState.canPublish) {
      throw new BadRequestException('errors.submit_for_publication.not_ready');
    }

    await req.datasetService.submitForPublication(datasetId, revision.id, user);
    const dataset = await DatasetRepository.getById(datasetId);

    res.status(201);
    res.json(DatasetDTO.fromDataset(dataset));
  } catch (err: unknown) {
    next(err);
  }
};

export const withdrawFromPublication = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const datasetId: string = res.locals.datasetId;
    const revision: Revision = res.locals.revision;
    const user = req.user as User;

    await req.datasetService.withdrawFromPublication(datasetId, revision.id, user);
    const withdrawnDataset = await DatasetRepository.getById(datasetId);
    res.status(201);
    res.json(DatasetDTO.fromDataset(withdrawnDataset));
  } catch (err: unknown) {
    logger.error(err, 'could not withdraw publication');
    next(err);
  }
};

export const regenerateRevisionCube = async (req: Request, res: Response, next: NextFunction) => {
  const datasetId: string = res.locals.datasetId;
  const revision: Revision = res.locals.revision;

  const datasetRelations: FindOptionsRelations<Dataset> = {
    revisions: {
      dataTable: true
    }
  };
  const dataset = await DatasetRepository.getById(datasetId, datasetRelations);
  const revisionTree = dataset.revisions
    .map((rev) => {
      if (rev.id === revision.id) return rev;
      else if (rev.revisionIndex > -1) return rev;
      else return undefined;
    })
    .filter((rev) => !!rev)
    .filter((rev) => !!rev?.dataTable);

  for (const rev of revisionTree) {
    if (!rev) {
      continue;
    }
    logger.debug(`Recreating datatable ${rev.dataTable?.id} in postgres data_tables database`);
    const tmpFilePath = await asyncTmpName({ postfix: rev.dataTable!.filename.split('.').reverse()[0] });
    const downloadStream = await req.fileService.loadStream(rev.dataTable!.filename, dataset.id);
    const writeStream = fs.createWriteStream(tmpFilePath);
    const dataTable = rev.dataTable!;
    const origEncoding = dataTable.encoding;

    await pipeline(downloadStream, writeStream).catch((err) => {
      logger.error(err, `An error occurred trying to save tmp local files for revision ${rev.id}`);
      next(new UnknownException('errors.download_from_filestore'));
      return;
    });

    const tmpFile: TempFile = {
      originalname: rev.dataTable!.originalFilename || 'unknown',
      mimetype: rev.dataTable!.mimeType,
      path: tmpFilePath
    };

    try {
      await extractTableInformation(tmpFile, rev.dataTable!, 'data_table');
    } catch (err) {
      logger.error(err, 'Something went wrong trying to process the CSV again and save to data_tables schema');
      next(err);
      return;
    }

    if (dataTable.fileType === FileType.Csv && dataTable.encoding !== origEncoding) {
      await dataTable.save();
    }
  }

  try {
    await createAllCubeFiles(datasetId, revision.id);
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube`);
    next(new UnknownException('errors.cube_builder.cube_build_failed'));
    return;
  }
  res.status(201);
  res.json({ message: 'Cube regeneration in postgres successful' });
};

// export const downloadRevisionCubeFile = async (req: Request, res: Response) => {
//   const datasetId: string = res.locals.datasetId;
//   const revision: Revision = res.locals.revision;
//
//   const cubeBuffer = await outputCube(
//     DuckdbOutputType.DuckDb,
//     datasetId,
//     revision.id,
//     req.language.split('-')[0],
//     req.fileService
//   );
//
//   logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
//   res.writeHead(200, {
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-Type': 'application/octet-stream',
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-disposition': `attachment;filename=${datasetId}.duckdb`,
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-Length': cubeBuffer.length
//   });
//   res.end(cubeBuffer);
// };

export const downloadRevisionCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
  const revision = res.locals.revision;
  const lang = req.language.split('-')[0];
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  try {
    createStreamingJSONFilteredView(res, revision, lang, sortByQuery, filterQuery);
  } catch (err) {
    logger.error(err);
    next(err);
  }
};

export const downloadRevisionCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
  const revision = res.locals.revision;
  const lang = req.language.split('-')[0];
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  try {
    createStreamingCSVFilteredView(res, revision, lang, sortByQuery, filterQuery);
  } catch (err) {
    logger.error(err);
    next(err);
  }
};

export const downloadRevisionCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
  const datasetId: string = res.locals.datasetId;
  const revision: Revision = res.locals.revision;

  if (!revision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }

  const cubeBuffer = await outputCube(
    DuckdbOutputType.Parquet,
    datasetId,
    revision.id,
    req.language.split('-')[0],
    req.fileService
  );

  logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.apache.parquet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${datasetId}.duckdb`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
  res.end(cubeBuffer);
};

export const downloadRevisionCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
  const revision = res.locals.revision;
  const lang = req.language.split('-')[0];
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  try {
    createStreamingExcelFilteredView(res, revision, lang, sortByQuery, filterQuery);
  } catch (err) {
    logger.error(err);
    next(err);
  }
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
