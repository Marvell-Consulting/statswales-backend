import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withAll } from '../repositories/published-dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { DownloadFormat } from '../enums/download-format';
import { BadRequestException } from '../exceptions/bad-request.exception';

import {
  downloadRevisionCubeAsCSV,
  downloadRevisionCubeAsExcel,
  downloadRevisionCubeAsParquet,
  downloadRevisionCubeFile
} from './revision';

export const listPublishedDatasets = async (req: Request, res: Response, next: NextFunction) => {
  logger.info('Listing published datasets...');
  try {
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 10;

    const results: ResultsetWithCount<DatasetListItemDTO> = await PublishedDatasetRepository.listPublishedByLanguage(
      lang,
      page,
      limit
    );

    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch published dataset list');
    next(new UnknownException());
  }
};

export const getPublishedDatasetById = async (req: Request, res: Response) => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  res.json(ConsumerDatasetDTO.fromDataset(dataset));
};

export const downloadPublishedDataset = async (req: Request, res: Response, next: NextFunction) => {
  const format = req.params.format;

  if (!format) {
    throw new BadRequestException('file format must be specified (csv, parquet, excel, duckdb)');
  }

  switch (format) {
    case DownloadFormat.Csv:
      downloadRevisionCubeAsCSV(req, res, next);
      return;

    case DownloadFormat.Parquet:
      downloadRevisionCubeAsParquet(req, res, next);
      return;

    case DownloadFormat.Excel:
      downloadRevisionCubeAsExcel(req, res, next);
      return;

    case DownloadFormat.DuckDb:
      downloadRevisionCubeFile(req, res, next);
      return;
  }

  next(new NotFoundException('invalid file format'));
};
