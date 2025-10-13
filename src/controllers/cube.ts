import { NextFunction, Request, Response } from 'express';

import { DatasetRepository, withDraftForCube } from '../repositories/dataset';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import {
  createStreamingCSVFilteredView,
  createStreamingExcelFilteredView,
  createStreamingJSONFilteredView
} from '../services/consumer-view';
import { SortByInterface } from '../interfaces/sort-by-interface';
import { FilterInterface } from '../interfaces/filterInterface';
import { logger } from '../utils/logger';
import { BadRequestException } from '../exceptions/bad-request.exception';

export const downloadCubeAsJSON = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const view = req.query.view as string;
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

  void createStreamingJSONFilteredView(res, latestRevision, req.language, view, sortBy, filter);
};

export const downloadCubeAsCSV = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const view = req.query.view as string;
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

  void createStreamingCSVFilteredView(res, latestRevision, req.language, view, sortBy, filter);
};

export const downloadCubeAsExcel = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = await DatasetRepository.getById(res.locals.datasetId, withDraftForCube);
  const latestRevision = getLatestRevision(dataset);
  if (!latestRevision) {
    next(new UnknownException('errors.no_revision'));
    return;
  }
  const view = req.query.view as string;
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

  void createStreamingExcelFilteredView(res, latestRevision, req.language, view, sortBy, filter);
};
