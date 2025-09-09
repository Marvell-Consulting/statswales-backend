import { NextFunction, Request, Response } from 'express';
import { TaskDTO } from '../dtos/task-dto';
import { TaskService } from '../services/task';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { ForbiddenException } from '../exceptions/forbidden.exception';
import { isApproverForDataset } from '../utils/get-permissions-for-user';
import { User } from '../entities/user/user';
import { logger } from '../utils/logger';
import { TaskDecisionDTO } from '../dtos/task-decision-dto';
import { dtoValidator } from '../validators/dto-validator';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';

export const getTask = async (req: Request, res: Response): Promise<void> => {
  res.json(TaskDTO.fromTask(res.locals.task));
};

export const taskDecision = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  let task = res.locals.task;
  const taskService = new TaskService();
  const dataset = task.dataset;
  const user = req.user! as User;

  if (!task.open) {
    next(new BadRequestException('errors.task.not_open'));
    return;
  }

  if (task.status !== TaskStatus.Requested) {
    next(new BadRequestException('errors.task.invalid_status'));
    return;
  }

  if (!isApproverForDataset(user, dataset)) {
    logger.warn(`User ${user.id} is not an approver for dataset ${dataset.id}`);
    next(new ForbiddenException('errors.task.user_is_not_approver_for_this_dataset'));
    return;
  }

  try {
    const dto = await dtoValidator(TaskDecisionDTO, req.body);
    logger.info(`Decision received for task ${task.id}: ${dto.decision}`);

    switch (task.action) {
      case TaskAction.Publish:
        if (dto.decision === 'approve') {
          // task is resolved and closed
          await req.datasetService.approvePublication(dataset.id, dataset.draftRevisionId, user);
          task = await taskService.update(task.id, TaskStatus.Approved, false, user);
        }
        if (dto.decision === 'reject') {
          // task left open so it can be re-submitted
          await req.datasetService.rejectPublication(dataset.id, dataset.draftRevisionId);
          task = await taskService.update(task.id, TaskStatus.Rejected, true, user, dto.reason);
        }
        break;

      case TaskAction.Unpublish:
        if (dto.decision === 'approve') {
          task = await taskService.approveUnpublish(task.id, user);
        }
        if (dto.decision === 'reject') {
          task = await taskService.rejectUnpublish(task.id, user, dto.reason);
        }
        break;

      case TaskAction.Archive:
        if (dto.decision === 'approve') {
          task = await taskService.approveArchive(task.id, user);
        }
        if (dto.decision === 'reject') {
          task = await taskService.rejectArchive(task.id, user, dto.reason);
        }
        break;

      case TaskAction.Unarchive:
        if (dto.decision === 'approve') {
          task = await taskService.approveUnarchive(task.id, user);
        }
        if (dto.decision === 'reject') {
          task = await taskService.rejectUnarchive(task.id, user, dto.reason);
        }
        break;

      default:
        next(new BadRequestException('errors.task.invalid_action'));
        return;
    }

    res.json(TaskDTO.fromTask(task));
  } catch (err) {
    next(err);
  }
};
