import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { UserGroupRepository } from '../repositories/user-group';
import { NotFoundException } from '../exceptions/not-found.exception';
import { hasError, userStatusValidator, uuidValidator } from '../validators';
import { arrayValidator, dtoValidator } from '../validators/dto-validator';
import { UserGroupMetadataDTO } from '../dtos/user/user-group-metadata-dto';
import { UserRepository } from '../repositories/user';
import { GroupRole } from '../enums/group-role';
import { UserDTO } from '../dtos/user/user-dto';
import { UserCreateDTO } from '../dtos/user/user-create-dto';
import { QueryFailedError } from 'typeorm';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { GlobalRole } from '../enums/global-role';
import { RoleSelectionDTO } from '../dtos/user/role-selection-dto';
import { UserStatus } from '../enums/user-status';

export const loadUserGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const userGroupIdError = await hasError(uuidValidator('user_group_id'), req);
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

export const loadUser = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const userIdError = await hasError(uuidValidator('user_id'), req);
  if (userIdError) {
    logger.error(userIdError);
    next(new NotFoundException('errors.user_id_invalid'));
    return;
  }

  try {
    const user = await UserRepository.getById(req.params.user_id);
    res.locals.user = user;
    res.locals.userId = user.id;
  } catch (error) {
    logger.error(error, 'Error loading user');
    next(new NotFoundException('errors.no_user'));
  }

  next();
};

export const listRoles = async (req: Request, res: Response): Promise<void> => {
  logger.info('List roles');
  res.json({
    global: Object.values(GlobalRole),
    group: Object.values(GroupRole)
  });
};

export const createUserGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Create new user group');
    const meta = await arrayValidator(UserGroupMetadataDTO, req.body);
    const group = await UserGroupRepository.createGroup(meta);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (err) {
    if (err instanceof BadRequestException) {
      next(err);
      return;
    }

    logger.error(err, 'Error creating group');
    next(new UnknownException());
  }
};

export const getAllUserGroups = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('get all user groups');
    const groups = await UserGroupRepository.getAll();
    res.json(groups.map((group) => UserGroupDTO.fromUserGroup(group, req.language as Locale)));
  } catch (err) {
    logger.error(err, 'Error getting groups');
    next(new UnknownException());
  }
};

export const listUserGroups = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Listing user groups with user and dataset counts');
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

export const getUserGroupById = async (req: Request, res: Response): Promise<void> => {
  const group = res.locals.userGroup;
  logger.debug(`Loading group: ${req.params.user_group_id}...`);
  res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
};

export const updateUserGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  let group = res.locals.userGroup;

  try {
    const dto = await dtoValidator(UserGroupDTO, req.body);
    group = await UserGroupRepository.updateGroup(group, dto);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (err) {
    if (err instanceof BadRequestException) {
      next(err);
      return;
    }

    logger.error(err, 'Error updating group');
    throw new UnknownException();
  }
};

export const listUsers = async (req: Request, res: Response): Promise<void> => {
  const page = parseInt(req.query.page as string, 10) || 1;
  const limit = parseInt(req.query.limit as string, 10) || 20;

  try {
    logger.info('List users');
    const lang = req.language as Locale;
    const results = await UserRepository.listByLanguage(lang, page, limit);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Error listing users');
    throw new UnknownException();
  }
};

export const createUser = async (req: Request, res: Response): Promise<void> => {
  try {
    logger.info('Creating new user...');
    const dto = await dtoValidator(UserCreateDTO, req.body);
    const user = await UserRepository.createUser(dto);
    res.json(UserDTO.fromUser(user, req.language as Locale));
  } catch (err) {
    logger.error(err, 'Error creating user');
    if (err instanceof QueryFailedError && err.message.includes('violates unique constraint')) {
      throw new BadRequestException('errors.user_already_exists');
    }
    throw new UnknownException();
  }
};

export const getUserById = async (req: Request, res: Response): Promise<void> => {
  const user = res.locals.user;
  logger.debug(`Loading user: ${req.params.user_id}...`);
  res.json(UserDTO.fromUser(user, req.language as Locale));
};

export const updateUserRoles = async (req: Request, res: Response): Promise<void> => {
  const userId: string = res.locals.userId;
  try {
    const roleSelections = await arrayValidator(RoleSelectionDTO, req.body);

    roleSelections.forEach((selection) => {
      if (!selection.roles) {
        throw new BadRequestException('errors.roles_required');
      }

      if (selection.type === 'group') {
        if (!selection.groupId) {
          throw new BadRequestException('errors.group_id_required');
        }

        if (!selection.roles.every((role: GroupRole) => Object.values(GroupRole).includes(role))) {
          throw new BadRequestException('errors.role_invalid');
        }
      }

      if (selection.type === 'global') {
        if (!selection.roles.every((role: GlobalRole) => Object.values(GlobalRole).includes(role))) {
          throw new BadRequestException('errors.role_invalid');
        }
      }
    });

    const user = await UserRepository.updateUserRoles(userId, roleSelections);
    res.json(UserDTO.fromUser(user, req.language as Locale));
  } catch (err) {
    logger.error(err, 'Error updating user roles');
    throw new UnknownException();
  }
};

export const updateUserStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const userId: string = res.locals.userId;

  const userStatusError = await hasError(userStatusValidator(), req);
  if (userStatusError) {
    logger.error(userStatusError);
    next(new BadRequestException('errors.user_status_invalid'));
    return;
  }

  const status = req.body.status as UserStatus;

  try {
    logger.info(`Updating user status: ${userId}...`);
    const user = await UserRepository.updateUserStatus(userId, status);
    res.json(UserDTO.fromUser(user, req.language as Locale));
  } catch (err) {
    logger.error(err, 'Error updating user status');
    throw new UnknownException();
  }
};
