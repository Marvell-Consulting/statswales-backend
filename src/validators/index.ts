import { Request } from 'express';
import { body, check, param, query, ValidationChain } from 'express-validator';
import { UserStatus } from '../enums/user-status';
import { DownloadFormat } from '../enums/download-format';
import { UserGroupStatus } from '../enums/user-group-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { OutputFormats } from '../enums/output-formats';
import { DatasetSimilarBy } from '../enums/dataset-similar-by';
import { SearchMode } from '../enums/search-mode';

export const hasError = async (validator: ValidationChain, req: Request): Promise<boolean> => {
  return !(await validator.run(req)).isEmpty();
};

export const datasetIdValidator = (): ValidationChain => param('dataset_id').trim().notEmpty().isUUID(4);

export const revisionIdValidator = (): ValidationChain => param('revision_id').trim().notEmpty().isUUID(4);

export const dimensionIdValidator = (): ValidationChain => param('dimension_id').trim().notEmpty().isUUID(4);

export const providerIdValidator = (): ValidationChain => param('provider_id').trim().notEmpty().isUUID(4);

export const uuidValidator = (paramName: string): ValidationChain => param(paramName).trim().notEmpty().isUUID(4);

export const titleValidator = (): ValidationChain => body('title').trim().notEmpty();

export const pageNumberValidator = (): ValidationChain =>
  check('page_number').optional().trim().notEmpty().isInt().toInt();

export const pageSizeValidator = (): ValidationChain => check('page_size').optional().trim().notEmpty().isInt().toInt();

export const filterIdValidator = (): ValidationChain => check('filter_id').trim().notEmpty().isString();

export const userStatusValidator = (): ValidationChain => body('status').isIn(Object.values(UserStatus));

export const groupStatusValidator = (): ValidationChain => body('status').isIn(Object.values(UserGroupStatus));

export const userGroupIdValidator = (userGroupIds: string[]): ValidationChain =>
  body('user_group_id').isIn(userGroupIds);

export const formatValidator = (): ValidationChain =>
  param('format').trim().notEmpty().isIn(Object.values(DownloadFormat));

export const format2Validator = (): ValidationChain =>
  check('format').optional().toLowerCase().trim().notEmpty().isIn(Object.values(OutputFormats));

export const buildTypeValidator = (): ValidationChain =>
  check('type').notEmpty().trim().isIn(Object.values(CubeBuildType));

export const buildStatusValidator = (): ValidationChain =>
  check('status').notEmpty().trim().isIn(Object.values(CubeBuildStatus));

export const similarByValidator = (): ValidationChain =>
  query('by').notEmpty().trim().isIn(Object.values(DatasetSimilarBy));

export const searchKeywordsValidator = (): ValidationChain => check('keywords').trim().notEmpty().isString();
export const searchModeValidator = (): ValidationChain => check('mode').optional().isIn(Object.values(SearchMode));
