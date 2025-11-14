import express, { Router, Request, Response, NextFunction } from 'express';

import { TaskService } from '../services/task';
import { getTask, taskDecision } from '../controllers/task';
import { NotFoundException } from '../exceptions/not-found.exception';
import { hasError, uuidValidator } from '../validators';
import { getUserGroupIdsForUser } from '../utils/get-permissions-for-user';
import { logger } from '../utils/logger';
import { ForbiddenException } from '../exceptions/forbidden.exception';

export const taskRouter = Router();

const jsonParser = express.json();

const taskAuth = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const taskIdError = await hasError(uuidValidator('task_id'), req);

  if (taskIdError) {
    next(new NotFoundException('errors.dataset_id_invalid'));
    return;
  }

  try {
    const taskService = new TaskService();
    const task = await taskService.getById(req.params.task_id, { dataset: true, createdBy: true, updatedBy: true });

    const dataset = task.dataset;
    const userGroupIds = getUserGroupIdsForUser(req.user!);

    logger.debug(`Checking user permissions for task ${task?.id}...`);

    if (!dataset || !dataset.userGroupId || !userGroupIds?.includes(dataset.userGroupId)) {
      logger.warn(`User does not have access to task ${task.id}`);
      next(new ForbiddenException('errors.dataset_not_in_users_groups'));
      return;
    }

    logger.debug(`User has access to task ${task.id} via dataset ${dataset.id} and group ${dataset.userGroupId}`);

    res.locals.task = task;
  } catch (error) {
    next(error);
  }
  next();
};

// ****** TASK AUTHORISATION MIDDLEWARE ****** //
// applies auth check for dataset for the current user
taskRouter.use('/:task_id', taskAuth);
// ***** DO NOT REMOVE ***** //

// GET /task/:task_id
// Returns a task by ID
taskRouter.get('/:task_id', getTask);

// PATCH /task/:task_id
// Updates a task when a decision is made
taskRouter.patch('/:task_id', jsonParser, taskDecision);
