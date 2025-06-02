import { NextFunction, Request, Response } from 'express';

import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { DatasetRepository, withDraftForCube } from '../repositories/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import { validateParams } from '../validators/preview-validator';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { QueryResult } from 'pg';
import { pool } from '../app';
import { StorageService } from '../interfaces/storage-service';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { createView } from '../services/consumer-view';
import { Revision } from '../entities/dataset/revision';

export const getPostgresCubePreview = async (
  revision: Revision,
  lang: string,
  dataset: Dataset,
  page: number,
  size: number,
  sortBy: SortByInterface[],
  filter?: FilterInterface[],
): Promise<ViewDTO | ViewErrDTO> => {
  try {
    return createView(dataset, revision, lang, page, size, sortBy, filter);
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube preview`);
    return { status: 500, errors: [], dataset_id: dataset.id };
  }
};

export const outputCube = async (
  mode: DuckdbOutputType,
  datasetId: string,
  revisionId: string,
  lang: string,
  storageService: StorageService
) => {
  try {
    if (mode === DuckdbOutputType.DuckDb) return storageService.loadBuffer(`${revisionId}.${mode}`, datasetId);
    else return storageService.loadBuffer(`${revisionId}_${lang}.${mode}`, datasetId);
  } catch (err) {
    logger.error(err, `Something went wrong trying to create the cube output file`);
    throw err;
  }
};

export const downloadCubeFile = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);

  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }

  const cubeBuffer = await outputCube(
    DuckdbOutputType.DuckDb,
    dataset.id,
    latestRevision.id,
    req.language.split('-')[0],
    req.fileService
  );

  logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/octet-stream',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
  res.end(cubeBuffer);
};

export const downloadCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const cubeBuffer = await outputCube(
    DuckdbOutputType.Json,
    dataset.id,
    latestRevision.id,
    req.language.split('-')[0],
    req.fileService
  );
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/json; charset=utf-8',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.json`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
  res.end(cubeBuffer);
};

export const downloadCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const cubeBuffer = await outputCube(
    DuckdbOutputType.Csv,
    dataset.id,
    latestRevision.id,
    req.language.split('-')[0],
    req.fileService
  );
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'text/csv; charset=utf-8',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.csv`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
  res.end(cubeBuffer);
};

export const downloadCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const cubeBuffer = await outputCube(
    DuckdbOutputType.Csv,
    dataset.id,
    latestRevision.id,
    req.language.split('-')[0],
    req.fileService
  );
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.apache.parquet',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.csv`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
};

export const downloadCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const cubeBuffer = await outputCube(
    DuckdbOutputType.Csv,
    dataset.id,
    latestRevision.id,
    req.language.split('-')[0],
    req.fileService
  );
  res.writeHead(200, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Type': 'application/vnd.ms-excel',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-disposition': `attachment;filename=${dataset.id}.csv`,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Content-Length': cubeBuffer.length
  });
};
