import { Request, Response, NextFunction } from 'express';
import { stringify } from 'csv-stringify';
import { subDays } from 'date-fns';

import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { UserGroupDTO } from '../dtos/user/user-group-dto';
import { UnknownException } from '../exceptions/unknown.exception';
import { UserGroupRepository } from '../repositories/user-group';
import { NotFoundException } from '../exceptions/not-found.exception';
import { groupStatusValidator, hasError, similarByValidator, userStatusValidator, uuidValidator } from '../validators';
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
import { UserGroupStatus } from '../enums/user-group-status';
import { DashboardStats, DatasetStats, UserGroupStats, UserStats } from '../interfaces/dashboard-stats';
import { DatasetStatsRepository } from '../repositories/dataset-stats';
import { DatasetSimilarBy } from '../enums/dataset-similar-by';
import { SearchLogRepository } from '../repositories/search-log';

export const loadUserGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const userGroupIdError = await hasError(uuidValidator('user_group_id'), req);
  if (userGroupIdError) {
    next(new NotFoundException('errors.user_group_id_invalid'));
    return;
  }

  try {
    const group = await UserGroupRepository.getById(req.params.user_group_id);
    res.locals.userGroup = group;
    res.locals.userGroupId = group.id;
  } catch (_err) {
    next(new NotFoundException('errors.no_user_group'));
  }

  next();
};

export const loadUser = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const userIdError = await hasError(uuidValidator('user_id'), req);
  if (userIdError) {
    next(new NotFoundException('errors.user_id_invalid'));
    return;
  }

  try {
    const user = await UserRepository.getById(req.params.user_id);
    res.locals.user = user;
    res.locals.userId = user.id;
  } catch (_err) {
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
  const status = req.query.status as UserGroupStatus | undefined;
  try {
    logger.info(`get all user groups with status: ${status || 'any'}`);
    const groups = await UserGroupRepository.getAll(status);
    res.json(groups.map((group) => UserGroupDTO.fromUserGroup(group, req.language as Locale)));
  } catch (err) {
    logger.error(err, 'Error getting groups');
    next(new UnknownException());
  }
};

export const listUserGroups = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const page = parseInt(req.query.page as string, 10) || 1;
  const limit = parseInt(req.query.limit as string, 10) || 20;
  const search = (req.query.search as string | undefined)?.trim().slice(0, 100);

  try {
    logger.info('Listing user groups with user and dataset counts');
    const lang = req.language as Locale;
    const results = await UserGroupRepository.listByLanguage(lang, page, limit, search);
    res.json(results);
  } catch (err) {
    logger.error(err, 'Error listing groups');
    next(new UnknownException());
  }
};

export const getUserGroupById = async (req: Request, res: Response): Promise<void> => {
  const groupId = res.locals.userGroupId;
  logger.debug(`Loading group: ${groupId}...`);
  const group = await UserGroupRepository.getByIdWithDatasets(groupId);
  res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
};

export const updateUserGroup = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const groupId = res.locals.userGroupId;

  try {
    const dto = await dtoValidator(UserGroupDTO, req.body);
    const group = await UserGroupRepository.updateGroup(groupId, dto);
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

export const updateUserGroupStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const groupId = res.locals.userGroupId;
  const status = req.body.status as UserGroupStatus;
  const group = await UserGroupRepository.getByIdWithDatasets(groupId);

  if (status === UserGroupStatus.Inactive && group.datasets && group.datasets.length > 0) {
    next(new BadRequestException('errors.group.cannot_deactivate_with_assigned_datasets'));
    return;
  }

  const groupStatusError = await hasError(groupStatusValidator(), req);
  if (groupStatusError) {
    next(new BadRequestException('errors.group.status_invalid'));
    return;
  }

  try {
    logger.info(`Updating user group status: ${groupId}...`);
    const group = await UserGroupRepository.updateGroupStatus(groupId, status);
    res.json(UserGroupDTO.fromUserGroup(group, req.language as Locale));
  } catch (err) {
    logger.error(err, 'Error updating user group status');
    throw new UnknownException();
  }
};

export const listUsers = async (req: Request, res: Response): Promise<void> => {
  const page = parseInt(req.query.page as string, 10) || 1;
  const limit = parseInt(req.query.limit as string, 10) || 20;
  const search = (req.query.search as string | undefined)?.trim().slice(0, 100);

  try {
    logger.info('List users');
    const lang = req.language as Locale;
    const results = await UserRepository.listByLanguage(lang, page, limit, search);
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
    if (err instanceof QueryFailedError && err.message.includes('violates unique constraint')) {
      throw new BadRequestException('errors.user_already_exists');
    }
    logger.error(err, 'Error creating user');
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

export const dashboard = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Getting dashboard statistics...');

    const [datasets, users, groups]: [DatasetStats, UserStats, UserGroupStats] = await Promise.all([
      DatasetStatsRepository.getDashboardStats(req.language as Locale),
      UserRepository.getDashboardStats(),
      UserGroupRepository.getDashboardStats()
    ]);

    const stats: DashboardStats = { datasets, users, groups };
    res.json(stats);
  } catch (err) {
    logger.error(err, 'Error getting dashboard statistics');
    next(new UnknownException());
  }
};

export const similarDatasets = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const similarBy = (req.query.by as DatasetSimilarBy) || DatasetSimilarBy.Sources;
  let csv: unknown[] = [];

  const similarByError = await hasError(similarByValidator(), req);
  if (similarByError) {
    next(new BadRequestException('errors.similar_by_invalid'));
    return;
  }

  try {
    switch (similarBy) {
      case DatasetSimilarBy.Sources: {
        logger.info('Getting similar datasets report by shared sources...');
        const sharedSources = await DatasetStatsRepository.shareSources(req.language as Locale);

        csv = sharedSources.map((row) => ({
          sources: row.sources.join('\n'),
          datasets_count: row.datasets_count,
          datasets: row.datasets.join('\n'),
          dataset_ids: row.dataset_ids.join('\n'),
          revision_ids: row.revision_ids.join('\n'),
          dimensions_count: row.dimensions_count ?? '',
          dimensions: row.dimensions ? row.dimensions.join('\n') : '',
          dimensions_common_count: row.dimensions_common_count ?? '',
          dimensions_common: row.dimensions_common ? row.dimensions_common.join('\n') : '',
          topics_count: row.topics_count ?? '',
          topics: row.topics ? row.topics.join('\n') : ''
        }));
        break;
      }

      case DatasetSimilarBy.Dimensions: {
        logger.info('Getting similar datasets report by shared dimensions...');
        const sharedDimensions = await DatasetStatsRepository.shareDimensions(req.language as Locale);

        csv = sharedDimensions.map((row) => ({
          dimensions: row.dimensions.join('\n'),
          datasets_count: row.datasets_count,
          datasets: row.datasets.join('\n'),
          dataset_ids: row.dataset_ids.join('\n')
        }));
        break;
      }

      case DatasetSimilarBy.Title: {
        logger.info('Getting similar datasets report by title similarity...');
        const similarTitles = await DatasetStatsRepository.similarTitles(req.language as Locale);

        csv = similarTitles.map((row) => ({
          title_1: row.title_1,
          title_2: row.title_2,
          similarity_score: row.similarity_score.toFixed(3)
        }));
        break;
      }

      case DatasetSimilarBy.Facts: {
        logger.info('Getting similar datasets report by same fact table...');
        const sameFactTables = await DatasetStatsRepository.sameFactTable(req.language as Locale);

        csv = sameFactTables.map((row) => ({
          original_filenames: row.original_filenames.join('\n'),
          datatable_hash: row.datatable_hash,
          count: row.count,
          datasets: row.datasets.join('\n')
        }));
        break;
      }

      default:
        next(new BadRequestException('errors.similar_by_invalid'));
        return;
    }

    res.setHeader('Content-Type', 'text/csv');
    stringify(csv, { bom: true, header: true, quoted_string: true }).pipe(res);
  } catch (err) {
    logger.error(err, 'Error getting similar datasets');
    next(new UnknownException());
  }
};

export const downloadSearchLogs = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    logger.info('Getting search logs report...');
    const start = req.query.start ? new Date(req.query.start as string) : subDays(new Date(), 30);
    const end = req.query.end ? new Date(req.query.end as string) : new Date();
    const logs = await SearchLogRepository.getByPeriod(start, end);

    const csv = logs.map((log) => ({
      timestamp: log.createdAt.toISOString(),
      mode: log.mode,
      keywords: log.keywords,
      result_count: log.resultCount ?? ''
    }));

    res.setHeader('Content-Type', 'text/csv');
    stringify(csv, { bom: true, header: true, quoted_string: true }).pipe(res);
  } catch (err) {
    logger.error(err, 'Error getting search logs');
    next(new UnknownException());
  }
};
