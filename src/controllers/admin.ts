import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { UserGroupRepository } from '../repositories/user-group';
import { NotFoundException } from '../exceptions/not-found.exception';
import { hasError, userGroupIdValidator } from '../validators';

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
  } catch (error) {
    logger.error(error, 'Error loading user group');
    next(new NotFoundException('errors.no_user_group'));
  }

  next();
};

export const createUserGroup = async (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('Create new user group');
    const { name_en, name_cy } = req.body;

    const group = await UserGroupRepository.createGroup(name_en, name_cy);
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
    logger.error(err, `Failed to load user group`);
    next(new NotFoundException('errors.no_user_group'));
  }
};

export const updateUserGroup = async (req: Request, res: Response, next: NextFunction) => {
  const group = res.locals.userGroup;

  next();
};
