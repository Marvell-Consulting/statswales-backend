import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { UserGroupRepository } from '../repositories/user-group';
import { NotFoundException } from '../exceptions/not-found.exception';
import { hasError, userGroupIdValidator } from '../validators';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { UserGroupMetadataDTO } from '../dtos/user/user-group-metadata-dto';

export const loadUserGroup = async (req: Request, res: Response, next: NextFunction) => {
  const userGroupIdError = await hasError(userGroupIdValidator(), req);
  if (userGroupIdError) {
    logger.error(userGroupIdError);
    next(new NotFoundException('errors.user_group_id_invalid'));
    return;
  }

  try {
    const group = await UserGroupRepository.getById(req.params.user_group_id);
    res.locals.userGroup = group;
    res.locals.userGroupId = group.id;
  } catch (error) {
    logger.error(error, 'Error loading user group');
    next(new NotFoundException('errors.no_user_group'));
  }

  next();
};

export const createUserGroup = async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('Create new user group');
    const meta = await arrayValidator(UserGroupMetadataDTO, req.body);
    const group = await UserGroupRepository.createGroup(meta);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (error) {
    logger.error('Error creating group', error);
    next(new UnknownException());
  }
};

export const listUserGroups = async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('List user groups');
    const lang = req.language as Locale;
    const page = parseInt(req.query.page as string, 10) || 1;
    const limit = parseInt(req.query.limit as string, 10) || 20;
    const results = await UserGroupRepository.listByLanguage(lang, page, limit);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Error listing groups');
    next(new UnknownException());
  }
};

export const getUserGroupById = async (req: Request, res: Response, next: NextFunction) => {
  const userGroupIdError = await hasError(userGroupIdValidator(), req);
  if (userGroupIdError) {
    logger.error(userGroupIdError);
    next(new NotFoundException('errors.user_group_id_invalid'));
    return;
  }

  try {
    logger.debug(`Loading group: ${req.params.user_group_id}...`);
    const group = await UserGroupRepository.getById(req.params.user_group_id);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (err) {
    logger.error(err, `Failed to get user group`);
    next(new NotFoundException('errors.no_user_group'));
  }
};

export const updateUserGroup = async (req: Request, res: Response) => {
  let group = res.locals.userGroup;

  try {
    const dto = await dtoValidator(UserGroupDTO, req.body);
    group = await UserGroupRepository.updateGroup(group, dto);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (err) {
    logger.error(err, 'Error updating group');
    throw new UnknownException();
  }
};
