import { Request, Response, NextFunction } from 'express';

import { logger } from '../utils/logger';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { NotFoundException } from '../exceptions/not-found.exception';
import { DatasetRepository } from '../repositories/dataset';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';
import { hasError, datasetIdValidator } from '../validators';
import { GlobalRole } from '../enums/global-role';

// middleware that loads the dataset, checks the user can view it, and stores it in res.locals.dataset
export const datasetAuth = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const datasetIdError = await hasError(datasetIdValidator(), req);
  if (datasetIdError) {
    logger.error(datasetIdError);
    next(new NotFoundException('errors.dataset_id_invalid'));
    return;
  }

  try {
    const dataset = await DatasetRepository.getById(req.params.dataset_id, {});
    const userGroupIds = getUserGroupIdsForUser(req.user!);
    const isDeveloper = req.user?.globalRoles.includes(GlobalRole.Developer);
    logger.debug(`Checking user permissions for dataset ${dataset.id}...`);

    if (isDeveloper) {
      logger.warn(`User ${req.user?.id} is a developer, skipping group permissions check`);
    } else if (!dataset.userGroupId || !userGroupIds?.includes(dataset.userGroupId)) {
      logger.warn(`User does not have access to dataset ${dataset.id}`);
      next(new ForbiddenException('errors.dataset_not_in_users_groups'));
      return;
    } else {
      logger.debug(`User has access to dataset ${dataset.id} via group ${dataset.userGroupId}`);
    }

    res.locals.datasetId = dataset.id;
    res.locals.dataset = dataset;
  } catch (err) {
    logger.error(err, `Failed to load dataset`);
    next(new NotFoundException('errors.no_dataset'));
    return;
  }

  next();
};
