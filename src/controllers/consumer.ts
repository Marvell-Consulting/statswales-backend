import { NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withAll } from '../repositories/published-dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import {
  createFrontendView,
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView,
  getFilters
} from '../services/consumer-view';
import { hasError, formatValidator } from '../validators';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { DownloadFormat } from '../enums/download-format';
import { DEFAULT_PAGE_SIZE } from '../utils/page-defaults';
import { UserGroupRepository } from '../repositories/user-group';
import { PublisherDTO } from '../dtos/publisher-dto';

export const listPublishedDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
    #swagger.summary = 'Get a list of all published datasets'
    #swagger.description = 'This endpoint returns a list of all published datasets.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size'
    ]
    #swagger.responses[200] = {
      description: 'A paginated list of all published datasets',
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
    const pageNumber = parseInt(req.query.page_number as string, 10) || 1;
    const pageSize = parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;

    const results = await PublishedDatasetRepository.listPublishedByLanguage(lang, pageNumber, pageSize);

    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch published dataset list');
    next(new UnknownException());
  }
};

export const getPublishedDatasetById = async (req: Request, res: Response): Promise<void> => {
  /*
    #swagger.summary = "Get a published dataset's metadata"
    #swagger.description = 'This endpoint returns all metadata for a published dataset.'
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A json object containing all metadata for a published dataset',
      schema: { $ref: "#/components/schemas/Dataset" }
    }
  */
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const datasetDTO = ConsumerDatasetDTO.fromDataset(dataset);

  if (dataset.userGroupId) {
    const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
    datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, req.language as Locale);
  }

  res.json(datasetDTO);
};

export const getPublishedDatasetView = async (req: Request, res: Response): Promise<void> => {
  /*
    #swagger.summary = 'Get a paginated view of a published dataset'
    #swagger.description = 'This endpoint returns a paginated view of a published dataset, with optional sorting and
      filtering.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter'
    ]
    #swagger.responses[200] = {
      description: 'A paginated view of a published dataset, with optional sorting and filtering',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetView" }
        }
      }
    }
  */
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const lang = req.language;

  if (!dataset.publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }

  const pageNumber: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const pageSize: number = Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE;
  let sortBy: SortByInterface[] | undefined;
  let filter: FilterInterface[] | undefined;

  try {
    sortBy = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing sort_by query parameters');
    throw new BadRequestException('errors.sort_by.invalid');
  }

  try {
    filter = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    throw new BadRequestException('errors.filter.invalid');
  }

  const preview = await createFrontendView(
    dataset,
    dataset.publishedRevision,
    lang,
    pageNumber,
    pageSize,
    sortBy,
    filter
  );

  res.json(preview);
};

export const getPublishedDatasetFilters = async (req: Request, res: Response): Promise<void> => {
  /*
    #swagger.summary = 'Get a list of the filters available for a paginated view of a published dataset'
    #swagger.description = 'This endpoint returns a list of the filters available for a paginated view of a published
      dataset. These are based on the variables used in the dataset, for example local authorities or financial years.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language'
    ]
    #swagger.responses[200] = {
      description: 'A list of the filters available for a paginated view of a published dataset',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/Filters" }
        }
      }
    }
  */
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, { publishedRevision: true });
  const lang = req.language.toLowerCase();

  logger.debug(JSON.stringify(req.headers));
  logger.debug(`Fetching filters for published dataset with language: ${lang}`);

  if (!dataset.publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }

  const filters = await getFilters(dataset.publishedRevision, lang || 'en-gb');
  res.json(filters);
};

export const downloadPublishedDataset = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
    #swagger.summary = 'Download a published dataset as a file'
    #swagger.description = 'This endpoint returns a published dataset file in a specified format.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id',
      '#/components/parameters/format',
      '#/components/parameters/sort_by',
      '#/components/parameters/filter'
    ]
    #swagger.responses[200] = {
      description: 'A published dataset file in a specified format',
      content: {
        'application/octet-stream': {
          schema: { type: 'string', format: 'binary', example: 'data.csv' }
        }
      }
    }
  */
  const formatError = await hasError(formatValidator(), req);

  if (formatError) {
    const availableFormats = Object.values(DownloadFormat).join(', ');
    next(new BadRequestException(`file format must be specified (${availableFormats})`));
    return;
  }

  const format = req.params.format;
  const view = req.query.view as string;
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  let sortBy: SortByInterface[] | undefined;
  let filter: FilterInterface[] | undefined;

  try {
    sortBy = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing sort_by query parameters');
    throw new BadRequestException('errors.sort_by.invalid');
  }

  try {
    filter = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    throw new BadRequestException('errors.filter.invalid');
  }

  const revision = dataset.publishedRevision;

  if (!revision?.onlineCubeFilename) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }

  try {
    switch (format as DuckdbOutputType) {
      case DuckdbOutputType.Csv:
        createStreamingCSVFilteredView(res, revision, req.language, view, sortBy, filter);
        break;
      case DuckdbOutputType.Json:
        createStreamingJSONFilteredView(res, revision, req.language, view, sortBy, filter);
        break;
      case DuckdbOutputType.Excel:
        createStreamingExcelFilteredView(res, revision, req.language, view, sortBy, filter);
        break;
      default:
        next(new BadRequestException('file format currently not supported'));
    }
  } catch (err) {
    logger.error(err, 'An error occurred trying to download published dataset');
    next(new UnknownException());
  }
};

export const listRootTopics = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
    #swagger.summary = 'Get a list of top-level topics'
    #swagger.description = "Datasets are tagged to topics. There are top-level topics, such as 'Health and social care',
      which can have sub-topics, such as 'Dental services'. This endpoint returns a list of all top-level topics that
      have at least one published dataset tagged to them."
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = ['#/components/parameters/language']
    #swagger.responses[200] = {
      description: 'A list of all top-level topics that have at least one published dataset tagged to them.',
      schema: { $ref: "#/components/schemas/RootTopics" }
    }
  */
  logger.info('fetching root level topics with at least one published dataset');

  try {
    const lang = req.language as Locale;
    const subTopics = await PublishedDatasetRepository.listPublishedTopics(lang);

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

export const listSubTopics = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
    #swagger.summary = 'Get a list of what sits under a given topic'
    #swagger.description = "Datasets are tagged to topics. There are top-level topics, such as 'Health and social
      care', which can have sub-topics, such as 'Dental services'. For a given topic_id, this endpoint returns a
      list of what sits under that topic - either sub-topics or published datasets tagged directly to that topic."
    #swagger.autoQuery = false
     #swagger.parameters['page_size'] = {
      description: 'Number of datasets per page when datasets are returned',
      in: 'query',
      type: 'integer',
      default: 1000
    }
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/topic_id',
      '#/components/parameters/page_number'
    ]
    #swagger.responses[200] = {
      description: 'A list of what sits under a given topic - either sub-topics or published datasets tagged directly
       to that topic.',
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
    const subTopics = await PublishedDatasetRepository.listPublishedTopics(lang, topicId);
    const parents = topic ? await TopicRepository.getParents(topic.path) : undefined;
    const isLeafTopic = topic && subTopics.length === 0;
    let datasets;

    if (isLeafTopic) {
      // if this is a leaf topic (no children) then also fetch datasets
      const pageNumber = parseInt(req.query.page_number as string, 10) || 1;
      const pageSize = parseInt(req.query.page_size as string, 10) || 1000;
      datasets = await PublishedDatasetRepository.listPublishedByTopic(topicId, lang, pageNumber, pageSize);
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

export const fixSwaggerDocGenerationWeirdness = (): void => {
  // Since we added return types to the functions above, Swagger-autogen has started to completely ignore the docs
  // for listRootTopics and listSubTopics actions. It's very strange and I do not know exactly why, possibly to do with
  // the code parser and colons?? At this point I do not have the time or inclination to debug third-party code to work
  // out the reason, and adding this useless function seems to fix the issue!
};
