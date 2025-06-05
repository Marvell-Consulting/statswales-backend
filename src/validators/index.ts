import { Request } from 'express';
import { body, check, param, ValidationChain } from 'express-validator';
import { UserStatus } from '../enums/user-status';
import { DownloadFormat } from '../enums/download-format';

export const hasError = async (validator: ValidationChain, req: Request) => {
  return !(await validator.run(req)).isEmpty();
};

export const datasetIdValidator = () => param('dataset_id').trim().notEmpty().isUUID(4);

export const revisionIdValidator = () => param('revision_id').trim().notEmpty().isUUID(4);

export const dimensionIdValidator = () => param('dimension_id').trim().notEmpty().isUUID(4);

export const providerIdValidator = () => param('provider_id').trim().notEmpty().isUUID(4);

export const uuidValidator = (paramName: string) => param(paramName).trim().notEmpty().isUUID(4);

export const titleValidator = () => body('title').trim().notEmpty();

export const pageNumberValidator = () => check('page_number').trim().notEmpty().isInt().toInt();

export const pageSizeValidator = () => check('page_size').trim().notEmpty().isInt().toInt();

export const userStatusValidator = () => body('status').isIn(Object.values(UserStatus));

export const userGroupIdValidator = (userGroupIds: string[]) => body('user_group_id').isIn(userGroupIds);

export const formatValidator = () => param('format').trim().notEmpty().isIn(Object.values(DownloadFormat));
