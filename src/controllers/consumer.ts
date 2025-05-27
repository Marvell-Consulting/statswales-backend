import { NextFunction, Request, Response } from 'express';

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
import tmp from 'tmp';
import fs from 'node:fs';
import { cleanUpCube } from '../services/cube-handler';
import { outputCube } from './cube-controller';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { createView } from '../services/consumer-view';
import { DEFAULT_PAGE_SIZE } from '../services/csv-processor';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';

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

export const getPublishedDatasetView = async (req: Request, res: Response) => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const lang = req.language.split('-')[0];
  if (!dataset.publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }
  const pageNumber: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const pageSize: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
  const sortBy = req.query.sort_by as string;

  const preview = await createView(dataset, dataset.publishedRevision, lang, pageNumber, pageSize, sortBy);
  res.json(preview);
};

export const downloadPublishedDataset = async (req: Request, res: Response, next: NextFunction) => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const format = req.params.format;

  if (!format) {
    throw new BadRequestException('file format must be specified (csv, parquet, excel, duckdb)');
  }

  const lang = req.language.split('-')[0];
  const revision = dataset.publishedRevision;
  if (!revision?.onlineCubeFilename) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }
  const fileBuffer = await req.fileService.loadBuffer(revision.onlineCubeFilename, dataset.id);
  const cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
  fs.writeFileSync(cubeFile, fileBuffer);
  let downloadFile: string;
  if ((format as DownloadFormat) === DownloadFormat.DuckDb) {
    downloadFile = cubeFile;
  } else {
    downloadFile = await outputCube(cubeFile, lang, format as DuckdbOutputType);
  }
  const downloadStream = fs.createReadStream(downloadFile);
  switch (format) {
    case DownloadFormat.Csv:
      // eslint-disable-next-line @typescript-eslint/naming-convention
      res.writeHead(200, { 'Content-Type': '\ttext/csv' });
      break;
    case DownloadFormat.Parquet:
      // eslint-disable-next-line @typescript-eslint/naming-convention
      res.writeHead(200, { 'Content-Type': '\tapplication/vnd.apache.parquet' });
      break;
    case DownloadFormat.Xlsx:
      // eslint-disable-next-line @typescript-eslint/naming-convention
      res.writeHead(200, { 'Content-Type': '\tapplication/vnd.ms-excel' });
      break;
    case DownloadFormat.DuckDb:
      // eslint-disable-next-line @typescript-eslint/naming-convention
      res.writeHead(200, { 'Content-Type': '\tapplication/octet-stream' });
      break;
    case DownloadFormat.Json:
      // eslint-disable-next-line @typescript-eslint/naming-convention
      res.writeHead(200, { 'Content-Type': '\tapplication/json' });
      break;
    default:
      next(new NotFoundException('invalid file format'));
      return;
  }
  downloadStream.pipe(res);

  // Handle errors in the file stream
  downloadStream.on('error', (err) => {
    logger.error(err, `File stream error: ${err}`);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    fs.unlinkSync(downloadFile);
    res.end('Server Error');
    cleanUpCube(cubeFile);
  });

  // Optionally listen for the end of the stream
  downloadStream.on('end', () => {
    fs.unlinkSync(downloadFile);
    logger.debug('File stream ended');
    cleanUpCube(cubeFile);
  });
};

export const listPublishedTopics = async (req: Request, res: Response, next: NextFunction) => {
  logger.info('fetching topics with at least one published dataset');
  const topicId = req.params.topic_id;
  const lang = req.language as Locale;

  if (topicId && !/\d+/.test(topicId)) {
    logger.error('invalid topic id');
    next(new BadRequestException('errors.invalid_topic_id'));
    return;
  }

  try {
    const topic = topicId ? await TopicRepository.findOneByOrFail({ id: parseInt(topicId, 10) }) : undefined;
    const subTopics = await PublishedDatasetRepository.listPublishedTopics(topicId);
    const parents = topic ? await TopicRepository.getParents(topic.path) : undefined;
    const isLeafTopic = topic && subTopics.length === 0;
    let datasets;

    if (isLeafTopic) {
      // if this is a leaf topic (no children) then also fetch datasets
      const page = parseInt(req.query.page as string, 10) || 1;
      const limit = parseInt(req.query.limit as string, 10) || 1000;
      datasets = await PublishedDatasetRepository.listPublishedByTopic(topicId, lang, page, limit);
    }

    const data: PublishedTopicsDTO = {
      selectedTopic: topic ? TopicDTO.fromTopic(topic, lang) : undefined,
      children: subTopics ? subTopics.map((topic) => TopicDTO.fromTopic(topic, lang)) : undefined,
      parents: parents ? parents.map((parent) => TopicDTO.fromTopic(parent, lang)) : undefined,
      datasets
    };

    res.json(data);
  } catch (error) {
    logger.error(error, 'Error listing published topics');
    next(new UnknownException());
  }
};
