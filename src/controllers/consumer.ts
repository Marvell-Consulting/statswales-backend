import { NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withAll } from '../repositories/published-dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { outputCube } from './cube-controller';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { createView, getFilters } from '../services/consumer-view';
import { DEFAULT_PAGE_SIZE } from '../services/csv-processor';
import { getDownloadHeaders } from '../utils/download-headers';
import { hasError, formatValidator } from '../validators';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';

export const listPublishedDatasets = async (req: Request, res: Response, next: NextFunction) => {
  /*
    #swagger.summary = 'List all published datasets'
    #swagger.description = 'Returns a paginated list of published datasets.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page',
      '#/components/parameters/limit'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of published datasets.',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetsWithCount" }
        }
      }
    }
  */
  logger.info('Listing published datasets...');

  try {
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 10;

    const results = await PublishedDatasetRepository.listPublishedByLanguage(lang, page, limit);

    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch published dataset list');
    next(new UnknownException());
  }
};

export const getPublishedDatasetById = async (req: Request, res: Response) => {
  /*
    #swagger.summary = 'Get a published dataset by ID'
    #swagger.description = 'Returns a single published dataset with all it\'s nested properities.'
    #swagger.parameters['$ref'] = ['#/components/parameters/dataset_id']
    #swagger.responses[200] = {
      description: 'A published dataset',
      schema: { $ref: "#/components/schemas/Dataset" }
    }
  */
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
  const sortByQuery = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  const filterQuery = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;

  const preview = await createView(
    dataset,
    dataset.publishedRevision,
    lang,
    pageNumber,
    pageSize,
    sortByQuery,
    filterQuery
  );

  res.json(preview);
};

export const getPublishedDatasetFilters = async (req: Request, res: Response) => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, {
    publishedRevision: true
  });
  const lang = req.language.toLowerCase();
  if (!dataset.publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }

  const filters = await getFilters(dataset.publishedRevision, lang);
  res.json(filters);
};

export const downloadPublishedDataset = async (req: Request, res: Response, next: NextFunction) => {
  const formatError = await hasError(formatValidator(), req);

  if (formatError) {
    next(new BadRequestException('file format must be specified (csv, parquet, excel, duckdb)'));
    return;
  }

  const format = req.params.format;
  const lang = req.language.split('-')[0];
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const revision = dataset.publishedRevision;

  if (!revision?.onlineCubeFilename) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }

  const fileBuffer = await outputCube(format as DuckdbOutputType, dataset.id, revision.id, lang, req.fileService);
  res.writeHead(200, getDownloadHeaders(dataset.id, format, fileBuffer.length));
  res.end(fileBuffer);
};

export const listRootTopics = async (req: Request, res: Response, next: NextFunction) => {
  /*
    #swagger.summary = 'List root (top-level) topics'
    #swagger.description = 'Datasets are hierarchically organized into topics. Each topic can have zero or more
      sub-topics. This endpoint returns a list of the root topics that have at least one published dataset.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/language']
    #swagger.responses[200] = {
      description: 'An object containing all root level topics (children). For root topics, the path is always equal
        to the id.',
      schema: { $ref: "#/components/schemas/RootTopics" }
    }
  */
  logger.info('fetching root level topics with at least one published dataset');

  try {
    const lang = req.language as Locale;
    const subTopics = await PublishedDatasetRepository.listPublishedTopics();

    const data: PublishedTopicsDTO = {
      selectedTopic: undefined,
      children: subTopics ? subTopics.map((topic) => TopicDTO.fromTopic(topic, lang)) : undefined,
      parents: undefined,
      datasets: undefined
    };

    res.json(data);
  } catch (error) {
    logger.error(error, 'Error listing published topics');
    next(new UnknownException());
  }
};

export const listSubTopics = async (req: Request, res: Response, next: NextFunction) => {
  /*
    #swagger.summary = 'List of sub-topics for a given topic'
    #swagger.description = 'Datasets are hierarchically organized into topics. Each topic can have zero or more
      sub-topics. This endpoint returns a list of the sub-topics of the topic specified by `topic_id` in the path.
      If the topic has no sub-topics, it will return the datasets for that topic instead.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page',
      '#/components/parameters/limit'
    ]
    #swagger.parameters['topic_id'] = {
      in: 'path',
      description: 'The ID of the topic to list child-topics for.',
      required: true,
      type: 'string',
      example: '1'
    }
    #swagger.responses[200] = {
      description: 'An object containing the selected topic, any sub-topics (children), any parent topics (parents)
        and if it has no sub-topics, any associated datasets. For sub-topics, the path includes the parent ids.',
      schema: { $ref: "#/components/schemas/PublishedTopics" }
    }
  */
  logger.info('fetching sub-topics with at least one published dataset');
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
