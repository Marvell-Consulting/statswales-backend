import { NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withPublishedRevision } from '../repositories/published-dataset';
import { PublishedRevisionRepository } from '../repositories/published-revision';
import { NotFoundException } from '../exceptions/not-found.exception';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { OutputFormats } from '../enums/output-formats';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { DEFAULT_PAGE_SIZE } from '../utils/page-defaults';
import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';
import {
  buildDataQuery,
  sendCsv,
  sendExcel,
  sendFrontendView,
  sendJson,
  sendFilters
} from '../services/consumer-view-v2';
import { Dataset } from '../entities/dataset/dataset';
import { DataOptionsDTO, DEFAULT_DATA_OPTIONS, FRONTEND_DATA_OPTIONS, PivotOptionsDTO } from '../dtos/data-options-dto';
import { SingleLanguageRevisionDTO } from '../dtos/consumer/single-language-revision-dto';
import { PageOptions } from '../interfaces/page-options';
import { dtoValidator } from '../validators/dto-validator';
import { QueryStoreRepository } from '../repositories/query-store';
import { QueryStore } from '../entities/query-store';
import { format2Validator, pageNumberValidator, pageSizeValidator } from '../validators';
import {
  getFilterTable,
  getFilterTableQuery,
  resolveDimensionToFactTableColumn,
  resolveFactColumnToDimension
} from '../utils/consumer';
import { sortObjToString } from '../utils/sort-obj-to-string';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { PublisherDTO } from '../dtos/publisher-dto';
import { UserGroupRepository } from '../repositories/user-group';
import { createPivotOutputUsingDuckDB, createPivotQuery, langToLocale } from '../services/pivots';
import { FieldValidationError, matchedData } from 'express-validator';
import { parsePageOptions } from '../utils/parse-page-options';
import { FieldValidationError, matchedData } from 'express-validator';
import { SearchMode } from '../enums/search-mode';
import { DatasetListItemDTO } from '../dtos/dataset-list-item-dto';
import { ResultsetWithCount } from '../interfaces/resultset-with-count';
import { searchKeywordsValidator, searchModeValidator } from '../validators';
import { SearchLog } from '../entities/search-log';

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
  const lang = req.language as Locale;
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withPublishedRevision);
  const datasetDTO = ConsumerDatasetDTO.fromDataset(dataset);

  if (dataset.userGroupId) {
    const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
    datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, lang);
  }

  res.json(datasetDTO);
};

export const getPublishedRevisionById = async (req: Request, res: Response): Promise<void> => {
  const lang = req.language as Locale;
  const revision = await PublishedRevisionRepository.getById(res.locals.revision_id);
  const revisionDto = SingleLanguageRevisionDTO.fromRevision(revision, lang);
  res.json(revisionDto);
};

async function parsePivotPageOptions(req: Request): Promise<PageOptions> {
  logger.debug('Parsing page options from request...');
  const validations = [format2Validator(), pageNumberValidator(), pageSizeValidator()];

  for (const validation of validations) {
    const result = await validation.run(req);
    if (!result.isEmpty()) {
      const error = result.array()[0] as FieldValidationError;
      throw new BadRequestException(`${error.msg} for ${error.path}`);
    }
  }

  const params = matchedData(req);
  let sort: string[] = [];

  try {
    const sortBy = req.query.sort_by ? (JSON.parse(req.query.sort_by as string) as SortByInterface[]) : undefined;
    sort = sortBy ? sortObjToString(sortBy) : [];
  } catch (_err) {
    throw new BadRequestException('errors.invalid_sort_by');
  }

  let xAxis: string | string[] = req.query.x as string;
  let yAxis: string | string[] = req.query.y as string;
  if (!xAxis || !yAxis) throw new BadRequestException('errors.invalid_pivot_params');
  xAxis = xAxis.split(',').map((x) => x.trim());
  yAxis = yAxis.split(',').map((y) => y.trim());
  if (xAxis.length === 1) xAxis = xAxis[0];
  if (yAxis.length === 1) yAxis = yAxis[0];

  return {
    x: xAxis,
    y: yAxis,
    format: (params.format as OutputFormats) ?? OutputFormats.Json,
    pageNumber: params.page_number ?? 1,
    pageSize: params.page_size ?? DEFAULT_PAGE_SIZE,
    sort,
    locale: req.language as Locale
  };
}

export const getPublishedDatasetData = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.debug(`Getting dataset data for ${res.locals.datasetId}...`);
  const filterId = req.params.filter_id as string | undefined;
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) return next(new NotFoundException('errors.no_published_revision'));

  try {
    const pageOptions = await parsePageOptions(req);
    const dataOptions = pageOptions.format === OutputFormats.Frontend ? FRONTEND_DATA_OPTIONS : DEFAULT_DATA_OPTIONS;

    const queryStore = filterId
      ? await QueryStoreRepository.getById(filterId)
      : await QueryStoreRepository.getByRequest(dataset.id, dataset.publishedRevisionId, dataOptions);

    const query = await buildDataQuery(queryStore, pageOptions);
    await sendFormattedResponse(query, queryStore, pageOptions, res);
  } catch (err) {
    if (res.headersSent) {
      logger.error(err, 'Error detected fetching data after headers already sent');
      return;
    }
    if (err instanceof NotFoundException || err instanceof BadRequestException) {
      return next(err);
    }
    logger.error(err, 'Error getting published dataset data');
    next(new UnknownException());
  }
};

// Hidden end point allows pivots on existing non-pivot queries and allows for multi-dimensional pivots
export const getPublishedDatasetPivot = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
  #swagger.ignore = true
   */
  logger.debug(`Getting dataset data for ${res.locals.datasetId}...`);
  const filterId = req.params.filter_id as string | undefined;
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) return next(new NotFoundException('errors.no_published_revision'));

  try {
    const pageOptions = await parsePivotPageOptions(req);
    const dataOptions = pageOptions.format === OutputFormats.Frontend ? FRONTEND_DATA_OPTIONS : DEFAULT_DATA_OPTIONS;

    const queryStore = filterId
      ? await QueryStoreRepository.getById(filterId)
      : await QueryStoreRepository.getByRequest(dataset.id, dataset.publishedRevisionId, dataOptions);

    const lang = langToLocale(pageOptions.locale);

    const pivotQuery = await createPivotQuery(lang, queryStore, pageOptions);
    await createPivotOutputUsingDuckDB(res, lang, pivotQuery, pageOptions, queryStore);
  } catch (err) {
    if (err instanceof NotFoundException || err instanceof BadRequestException) {
      return next(err);
    }
    logger.error(err, 'Error getting published dataset data');
    next(new UnknownException());
  }
};

export const getPublishedDatasetPivotFromId = async (
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> => {
  logger.debug(`Getting dataset data for ${res.locals.datasetId}...`);
  const filterId = req.params.filter_id as string | undefined;
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) return next(new NotFoundException('errors.no_published_revision'));

  try {
    const pageOptions = await parsePivotPageOptions(req);
    const dataOptions = pageOptions.format === OutputFormats.Frontend ? FRONTEND_DATA_OPTIONS : DEFAULT_DATA_OPTIONS;

    const queryStore = filterId
      ? await QueryStoreRepository.getById(filterId)
      : await QueryStoreRepository.getByRequest(dataset.id, dataset.publishedRevisionId, dataOptions);

    if (!queryStore.requestObject.pivot) {
      throw new BadRequestException('errors.not_a_pivot_filter');
    }

    pageOptions.x = queryStore.requestObject.pivot.x;
    pageOptions.y = queryStore.requestObject.pivot.y;

    const lang = langToLocale(pageOptions.locale);

    const pivotQuery = await createPivotQuery(lang, queryStore, pageOptions);
    await createPivotOutputUsingDuckDB(res, lang, pivotQuery, pageOptions, queryStore);
  } catch (err) {
    if (err instanceof NotFoundException || err instanceof BadRequestException) {
      return next(err);
    }
    logger.error(err, 'Error getting published dataset data');
    next(new UnknownException());
  }
};

export const generatePivotFilterId = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.debug(`Generating filter ID for published dataset ${res.locals.datasetId}...`);
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) return next(new NotFoundException('errors.no_published_revision'));

  const dataOptions = await dtoValidator(PivotOptionsDTO, req.body);

  if (dataOptions.pivot.x.split(',').length > 1 || dataOptions.pivot.y.split(',').length > 1) {
    throw new BadRequestException('errors.pivot_only_one_column');
  }

  const lang = langToLocale(dataOptions.locale);
  const filterTable = await getFilterTable(dataset.publishedRevisionId);

  let xCol = dataOptions.pivot.x;
  let yCol = dataOptions.pivot.y;
  if (dataOptions.options.use_raw_column_names) {
    try {
      xCol = resolveFactColumnToDimension(xCol, lang, filterTable);
    } catch (_) {
      throw new BadRequestException('X Column not found in dataset');
    }
    try {
      yCol = resolveFactColumnToDimension(yCol, lang, filterTable);
    } catch (_) {
      throw new BadRequestException('Y Column not found in dataset');
    }
  } else {
    try {
      xCol = resolveDimensionToFactTableColumn(xCol, filterTable);
    } catch (_) {
      throw new BadRequestException('X Column not found in dataset');
    }
    try {
      yCol = resolveDimensionToFactTableColumn(yCol, filterTable);
    } catch (_) {
      throw new BadRequestException('Y Column not found in dataset');
    }
  }

  if (dataOptions?.filters && dataOptions?.filters.length > 0) {
    for (const filter of dataOptions.filters) {
      let colName = Object.keys(filter)[0];
      const filterValues = Object.values(filter)[0] as string[];
      if (dataOptions.options.use_raw_column_names) {
        colName = resolveFactColumnToDimension(colName, lang, filterTable);
      } else {
        colName = resolveDimensionToFactTableColumn(colName, filterTable);
      }

      if (colName === xCol || colName === yCol) {
        continue;
      }

      if (filterValues.length > 0) {
        throw new BadRequestException('Non X and Y columns must contain only one value');
      }
    }
  }

  try {
    const queryStore = await QueryStoreRepository.getByRequest(dataset.id, dataset.publishedRevisionId, dataOptions);
    res.json({ filterId: queryStore.id });
  } catch (err) {
    logger.error(err, 'Error generating filter ID');
    return next(new UnknownException());
  }
};

export const generateFilterId = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.info(`Generating filter ID for published dataset ${res.locals.datasetId}...`);
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) return next(new NotFoundException('errors.no_published_revision'));

  try {
    const dataOptions = await dtoValidator(DataOptionsDTO, req.body);
    const queryStore = await QueryStoreRepository.getByRequest(dataset.id, dataset.publishedRevisionId, dataOptions);
    res.json({ filterId: queryStore.id });
  } catch (err) {
    if (err instanceof NotFoundException || err instanceof BadRequestException) {
      return next(err);
    }
    logger.error(err, 'Error generating filter ID');
    return next(new UnknownException());
  }
};

export const getPublishedDatasetFilters = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.debug('Getting published dataset filters...');
  const dataset = res.locals.dataset as Dataset;
  if (!dataset.publishedRevisionId) throw new NotFoundException('errors.no_published_revision');

  try {
    const locale = req.language as Locale;
    const query = await getFilterTableQuery(dataset.publishedRevisionId, locale);
    await sendFilters(query, res);
  } catch (err) {
    if (err instanceof NotFoundException || err instanceof BadRequestException) {
      return next(err);
    }
    next(err);
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

export const sendFormattedResponse = async (
  query: string,
  queryStore: QueryStore,
  pageOptions: PageOptions,
  res: Response
): Promise<void> => {
  switch (pageOptions.format) {
    case OutputFormats.Frontend:
      return sendFrontendView(query, queryStore, pageOptions, res);
    case OutputFormats.Csv:
      return sendCsv(query, queryStore, res);
    case OutputFormats.Excel:
      return sendExcel(query, queryStore, res);
    case OutputFormats.Json:
      return sendJson(query, queryStore, res);
    default:
      res.status(400).json({ error: 'Format not supported' });
  }
};

export const searchPublishedDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  /*
    #swagger.summary = 'Search published datasets'
    #swagger.description = 'This endpoint performs a full-text search across published dataset titles and summaries.'
    #swagger.autoQuery = false
    #swagger.parameters['$ref'] = [
      '#/components/parameters/language',
      '#/components/parameters/page_number',
      '#/components/parameters/page_size'
    ]
    #swagger.parameters['keywords'] = {
      in: 'query',
      description: 'Search query string',
      required: true,
      schema: { type: 'string' }
    }
    #swagger.responses[200] = {
      description: 'A paginated list of matching published datasets',
      content: {
        'application/json': {
          schema: { $ref: "#/components/schemas/DatasetsWithCount" }
        }
      }
    }
  */

  try {
    for (const validation of [searchKeywordsValidator(), searchModeValidator()]) {
      const result = await validation.run(req);
      if (!result.isEmpty()) {
        const error = result.array()[0] as FieldValidationError;
        throw new BadRequestException(`${error.msg} for ${error.path}`);
      }
    }

    const { mode = SearchMode.Basic, keywords } = matchedData(req);
    const { pageNumber, pageSize, locale } = await parsePageOptions(req);
    logger.info(`Searching published datasets with mode: ${mode} keywords: ${keywords} lang: ${locale}`);

    const results: ResultsetWithCount<DatasetListItemDTO> =
      mode === SearchMode.FTS
        ? await PublishedDatasetRepository.searchFTS(locale, keywords, pageNumber, pageSize)
        : await PublishedDatasetRepository.searchBasic(locale, keywords, pageNumber, pageSize);

    await SearchLog.create({ keywords, resultCount: results.count }).save();

    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to search published datasets');
    next(new UnknownException());
  }
};
