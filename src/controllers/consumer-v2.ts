import { NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withPublishedRevision } from '../repositories/published-dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { ConsumerOutFormats } from '../enums/consumer-output-formats';
import { format2Validator, hasError } from '../validators';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { DEFAULT_PAGE_SIZE } from '../utils/page-defaults';
import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';
import { DatasetDTO } from '../dtos/consumer/dataset';
import { FullRevision } from '../dtos/consumer/revision';
import {
  createQueryStoreEntry,
  sendConsumerDataToUser,
  sendConsumerDataToUserNoFilter,
  sendFilterTableToUser
} from '../services/consumer-view-v2';
import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { ConsumerOptions } from '../interfaces/consumer-options';

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
    #swagger.description = 'This endpoint returns all consumer metadata for a published dataset.'
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/dataset_id'
    ]
    #swagger.responses[200] = {
      description: 'A json object containing all metadata for a published dataset',
      schema: { $ref: "#/components/schemas/Dataset" }
    }
  */
  const language = req.query.languague ? (req.query.languague as Locale) : ('en-GB' as Locale);
  const datasetDTO = await DatasetDTO.fromDatasetId(res.locals.datasetId, language);
  res.json(datasetDTO);
};

export const getPublishedRevisionById = async (req: Request, res: Response): Promise<void> => {
  const language = req.query.languague ? (req.query.languague as Locale) : ('en-GB' as Locale);
  const revisionDto = FullRevision.fromRevision(res.locals.revision, language);
  res.json(revisionDto);
};

async function apiSetup(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<{
  dataset: Dataset;
  publishedRevision?: Revision;
  sort?: string[];
  format?: ConsumerOutFormats;
  pageNumber?: number;
  pageSize?: number;
  language: Locale;
  errors: boolean;
}> {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withPublishedRevision);
  const publishedRevision = dataset.publishedRevision;
  const language = req.language as Locale;

  if (!publishedRevision) {
    next(new NotFoundException('errors.no_revision'));
    return {
      dataset,
      language,
      errors: true
    };
  }

  let format: ConsumerOutFormats | undefined;

  if (req.query.format) {
    logger.trace(`Format = ${req.query.format}`);
    const formatError = await hasError(format2Validator(), req);

    if (formatError) {
      const availableFormats = Object.values(ConsumerOutFormats).join(', ').replace('filter, ', '');
      next(new BadRequestException(`file format must be specified (${availableFormats})`));
      return {
        dataset,
        language,
        errors: true
      };
    }

    format = req.query.format as ConsumerOutFormats;

    if (format === ConsumerOutFormats.Filter) {
      next(new BadRequestException(`Filter is only available on the filter endpoint.`));
      return {
        dataset,
        language,
        errors: true
      };
    }
  }

  const pageNumber: number | undefined = req.query.page_number
    ? Number.parseInt(req.query.page_number as string, 10)
    : undefined;
  const pageSize: number | undefined = req.query.page_size
    ? Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE
    : undefined;
  const sort: string[] | undefined = req.query.sort ? (req.query.sort as string).split(',') : undefined;
  return {
    dataset,
    publishedRevision,
    language,
    format,
    sort,
    pageNumber,
    pageSize,
    errors: false
  };
}

export const getPublishedDatasetViewNoFilters = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { dataset, language, format, sort, pageNumber, pageSize, errors } = await apiSetup(req, res, next);
  if (errors) return;
  await sendConsumerDataToUserNoFilter(res, next, language, dataset, pageNumber, pageSize, format, sort);
};

export const getPublishedDatasetViewFilters = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { dataset, language, format, sort, pageNumber, pageSize, errors } = await apiSetup(req, res, next);
  if (errors) return;
  const filterId: string | undefined = req.params.filter_id ? (req.params.filter_id as string) : undefined;
  logger.debug(`Filter ID = ${filterId}`);
  if (!filterId) {
    next(new NotFoundException('errors.no_filter_id'));
    return;
  }
  await sendConsumerDataToUser(res, next, language, dataset, filterId, pageNumber, pageSize, format, sort);
};

export const generateFilterId = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const { dataset, publishedRevision, errors } = await apiSetup(req, res, next);
  if (errors) return;

  let consumerOptions: ConsumerOptions | undefined;
  try {
    logger.debug(`req body = ${JSON.stringify(req.body)}`);
    consumerOptions = req.body ? (req.body as ConsumerOptions) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    next(new BadRequestException('errors.bad_json'));
    return;
  }
  if (!consumerOptions) {
    next(new BadRequestException('errors.filter.missing'));
    return;
  }
  await createQueryStoreEntry(res, next, dataset, publishedRevision!, consumerOptions);
};

export const getPublishedDatasetFilters = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const { publishedRevision, language, errors } = await apiSetup(req, res, next);
  if (errors) return;
  let format: ConsumerOutFormats | undefined;
  if (req.query.format) {
    const formatError = await hasError(format2Validator(), req);

    if (formatError) {
      const availableFormats = Object.values(ConsumerOutFormats).join(', ');
      next(new BadRequestException(`file format must be specified (${availableFormats})`));
      return;
    }

    format = req.query.format as ConsumerOutFormats;
  }
  await sendFilterTableToUser(res, next, language, publishedRevision!, format);
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
    #swagger.parameters['sort_by'] = {
      description: `Columns to sort the data by. The value should be a JSON array of objects sent as a URL encoded string.`,
      in: 'query',
      required: false,
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                columnName: {
                  type: 'string',
                  enum: ['first_published_at', 'last_updated_at', 'title']
                },
                direction: {
                  type: 'string',
                  enum: ['ASC', 'DESC']
                }
              }
            }
          }
        }
      }
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
    next(new NotFoundException('errors.invalid_topic_id'));
    return;
  }

  const sortBy: SortByInterface[] = req.query.sort_by ? JSON.parse(req.query.sort_by as string) : [];

  sortBy.forEach((sort) => {
    if (!['first_published_at', 'last_updated_at', 'title'].includes(sort.columnName)) {
      throw new BadRequestException('errors.invalid_sort_by');
    }
  });

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
      datasets = await PublishedDatasetRepository.listPublishedByTopic(topicId, lang, pageNumber, pageSize, sortBy);
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

export const getPublicationHistory = async (req: Request, res: Response): Promise<void> => {
  /*
    #swagger.ignore = true
  */
  const revisions = await PublishedDatasetRepository.getHistoryById(res.locals.datasetId);
  const revisionDTOs = revisions.map((rev) => ConsumerRevisionDTO.fromRevision(rev));

  res.json(revisionDTOs);
};

export const fixSwaggerDocGenerationWeirdness = (): void => {
  // Since we added return types to the functions above, Swagger-autogen has started to completely ignore the docs
  // for listRootTopics and listSubTopics actions. It's very strange and I do not know exactly why, possibly to do with
  // the code parser and colons?? At this point I do not have the time or inclination to debug third-party code to work
  // out the reason, and adding this useless function seems to fix the issue!
};
