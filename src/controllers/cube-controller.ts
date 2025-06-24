import { NextFunction, Request, Response } from 'express';

import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { DatasetRepository, withDraftForCube } from '../repositories/dataset';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import { outputCube } from '../services/cube-handler';

// export const downloadCubeFile = async (req: Request, res: Response, next: NextFunction) => {
//   const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
//   const latestRevision = getLatestRevision(dataset);
//
//   if (!latestRevision) {
//     next(new UnknownException('errors.no_revision'));
//     return;
//   }
//
//   const cubeBuffer = await outputCube(
//     DuckdbOutputType.DuckDb,
//     dataset.id,
//     latestRevision.id,
//     req.language.split('-')[0],
//     req.fileService
//   );
//
//   logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
//   res.writeHead(200, {
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-Type': 'application/octet-stream',
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
//     // eslint-disable-next-line @typescript-eslint/naming-convention
//     'Content-Length': cubeBuffer.length
//   });
//   res.end(cubeBuffer);
// };

export const downloadCubeAsJSON = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const downloadCubeAsCSV = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const downloadCubeAsParquet = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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

export const downloadCubeAsExcel = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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
