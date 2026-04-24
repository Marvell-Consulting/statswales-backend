import { NextFunction, Request, Response } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UnknownException } from '../exceptions/unknown.exception';
import { PublishedDatasetRepository, withAll } from '../repositories/published-dataset';
import { NotFoundException } from '../exceptions/not-found.exception';
import { ConsumerDatasetDTO } from '../dtos/consumer-dataset-dto';
import { BadRequestException } from '../exceptions/bad-request.exception';
import {
  createFrontendView,
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView,
  createStreamingPostgresPivotView,
  getFilters
} from '../services/consumer-view';
import { hasError, formatValidator } from '../validators';
import { TopicDTO } from '../dtos/topic-dto';
import { PublishedTopicsDTO } from '../dtos/published-topics-dto';
import { TopicRepository } from '../repositories/topic';
import { FilterInterface } from '../interfaces/filterInterface';
import { parseSortByToObjects } from '../utils/parse-sort-by-param';
import { DownloadFormat } from '../enums/download-format';
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from '../utils/page-defaults';
import { clamp } from '../utils/clamp';
import { UserGroupRepository } from '../repositories/user-group';
import { PublisherDTO } from '../dtos/publisher-dto';
import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';

export const listPublishedDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  logger.info('Listing published datasets...');

  try {
    const lang = req.language as Locale;
    const pageNumber = parseInt(req.query.page_number as string, 10) || 1;
    const pageSize = clamp(parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE);

    const results = await PublishedDatasetRepository.listPublishedByLanguage(lang, pageNumber, pageSize);

    res.json(results);
  } catch (err) {
    logger.error(err, 'Failed to fetch published dataset list');
    next(new UnknownException());
  }
};

export const getPublishedDatasetById = async (req: Request, res: Response): Promise<void> => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const datasetDTO = ConsumerDatasetDTO.fromDataset(dataset);

  if (dataset.userGroupId) {
    const userGroup = await UserGroupRepository.getByIdWithOrganisation(dataset.userGroupId);
    datasetDTO.publisher = PublisherDTO.fromUserGroup(userGroup, req.language as Locale);
  }

  res.json(datasetDTO);
};

export const getPublishedDatasetView = async (req: Request, res: Response): Promise<void> => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  const publishedRevision = dataset.publishedRevision;
  const lang = req.language;

  if (!publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }

  const pageNumber: number = Number.parseInt(req.query.page_number as string, 10) || 1;
  const pageSize: number = clamp(
    Number.parseInt(req.query.page_size as string, 10) || DEFAULT_PAGE_SIZE,
    1,
    MAX_PAGE_SIZE
  );
  let filter: FilterInterface[] | undefined;

  const sortBy = parseSortByToObjects(req.query.sort_by as string);

  try {
    filter = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    throw new BadRequestException('errors.filter.invalid');
  }

  try {
    const preview = await createFrontendView(dataset, publishedRevision.id, lang, pageNumber, pageSize, sortBy, filter);
    res.status(200).json(preview);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to query the cube');
    throw new UnknownException('errors.consumer_view.cube_query_failed');
  }
};

export const getPublishedDatasetFilters = async (req: Request, res: Response): Promise<void> => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, { publishedRevision: true });
  const publishedRevision = dataset.publishedRevision;
  const lang = req.language.toLowerCase();
  logger.debug(`Fetching filters for published dataset with language: ${lang}`);

  if (!publishedRevision) {
    throw new NotFoundException('errors.no_revision');
  }

  const filters = await getFilters(publishedRevision.id, lang || 'en-gb');
  res.json(filters);
};

export const downloadPublishedDataset = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const formatError = await hasError(formatValidator(), req);

  if (formatError) {
    const availableFormats = Object.values(DownloadFormat).join(', ');
    next(new BadRequestException(`file format must be specified (${availableFormats})`));
    return;
  }

  const format = req.params.format;
  const view = req.query.view as string;
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);
  let filter: FilterInterface[] | undefined;

  const sortBy = parseSortByToObjects(req.query.sort_by as string);

  try {
    filter = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    throw new BadRequestException('errors.filter.invalid');
  }

  const publishedRevision = dataset.publishedRevision;

  if (!publishedRevision?.onlineCubeFilename) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }

  try {
    switch (format as DownloadFormat) {
      case DownloadFormat.Csv:
        createStreamingCSVFilteredView(res, publishedRevision.id, req.language, view, sortBy, filter);
        break;
      case DownloadFormat.Json:
        createStreamingJSONFilteredView(res, publishedRevision.id, req.language, view, sortBy, filter);
        break;
      case DownloadFormat.Xlsx:
        createStreamingExcelFilteredView(res, publishedRevision.id, req.language, view, sortBy, filter);
        break;
      default:
        next(new BadRequestException('file format currently not supported'));
    }
  } catch (err) {
    logger.error(err, 'An error occurred trying to download published dataset');
    next(new UnknownException());
  }
};

export const getPostgresPivotTable = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await PublishedDatasetRepository.getById(res.locals.datasetId, withAll);

  let filter: FilterInterface[] | undefined;
  try {
    filter = req.query.filter ? (JSON.parse(req.query.filter as string) as FilterInterface[]) : undefined;
  } catch (err) {
    logger.warn(err, 'Error parsing filter query parameters');
    throw new BadRequestException('errors.filter.invalid');
  }

  const xAxis = req.query.x?.toString();
  if (!xAxis) {
    logger.warn(`No X Axis present`);
    throw new BadRequestException('No X Axis present');
  }

  const yAxis = req.query.y?.toString();
  if (!yAxis) {
    logger.warn(`No Y Axis present`);
    throw new BadRequestException('No Y Axis present');
  }

  const publishedRevision = dataset.publishedRevision;

  if (!publishedRevision?.onlineCubeFilename) {
    next(new NotFoundException('errors.no_revision'));
    return;
  }
  try {
    void createStreamingPostgresPivotView(res, publishedRevision.id, req.language, xAxis, yAxis, filter);
  } catch (err) {
    logger.error(err, 'An error occurred trying to produce postgres pivot as JSON');
    next(new UnknownException());
  }
};

export const listRootTopics = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
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
  logger.info('fetching sub-topics with at least one published dataset');
  const topicId = req.params.topic_id;
  const lang = req.language as Locale;

  if (topicId && !/^\d+$/.test(topicId)) {
    next(new NotFoundException('errors.invalid_topic_id'));
    return;
  }

  const allowedColumns = ['first_published_at', 'last_updated_at', 'title'];
  const sortBy = parseSortByToObjects(req.query.sort_by as string) ?? [];

  sortBy.forEach((sort) => {
    if (!allowedColumns.includes(sort.columnName)) {
      throw new BadRequestException('errors.invalid_sort_by');
    }
  });

  const topic = topicId ? await TopicRepository.findOneBy({ id: parseInt(topicId, 10) }) : undefined;

  if (topicId && !topic) {
    next(new NotFoundException('errors.topic_not_found'));
    return;
  }

  try {
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
  const revisions = await PublishedDatasetRepository.getHistoryById(res.locals.datasetId);
  const revisionDTOs = revisions.map((rev) => ConsumerRevisionDTO.fromRevision(rev));

  res.json(revisionDTOs);
};
