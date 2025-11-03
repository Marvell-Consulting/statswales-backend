import { Request } from 'express';
import { body, check, param, ValidationChain } from 'express-validator';
import { UserStatus } from '../enums/user-status';
import { DownloadFormat } from '../enums/download-format';
import { UserGroupStatus } from '../enums/user-group-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { CubeBuildStatus } from '../enums/cube-build-status';

export const hasError = async (validator: ValidationChain, req: Request): Promise<boolean> => {
  return !(await validator.run(req)).isEmpty();
};

export const datasetIdValidator = (): ValidationChain => param('dataset_id').trim().notEmpty().isUUID(4);

export const revisionIdValidator = (): ValidationChain => param('revision_id').trim().notEmpty().isUUID(4);

export const dimensionIdValidator = (): ValidationChain => param('dimension_id').trim().notEmpty().isUUID(4);

export const providerIdValidator = (): ValidationChain => param('provider_id').trim().notEmpty().isUUID(4);

export const uuidValidator = (paramName: string): ValidationChain => param(paramName).trim().notEmpty().isUUID(4);

export const titleValidator = (): ValidationChain => body('title').trim().notEmpty();

export const pageNumberValidator = (): ValidationChain => check('page_number').trim().notEmpty().isInt().toInt();

export const pageSizeValidator = (): ValidationChain => check('page_size').trim().notEmpty().isInt().toInt();

export const userStatusValidator = (): ValidationChain => body('status').isIn(Object.values(UserStatus));
export const groupStatusValidator = (): ValidationChain => body('status').isIn(Object.values(UserGroupStatus));

export const userGroupIdValidator = (userGroupIds: string[]): ValidationChain =>
  body('user_group_id').isIn(userGroupIds);

export const formatValidator = (): ValidationChain =>
  param('format').trim().notEmpty().isIn(Object.values(DownloadFormat));

export const buildTypeValidator = (): ValidationChain => param('type').trim().isIn(Object.values(CubeBuildType));

export const buildStatusValidator = (): ValidationChain => param('start').trim().isIn(Object.values(CubeBuildStatus));
