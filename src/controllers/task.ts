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

export const getTask = async (req: Request, res: Response) => {
  res.json(TaskDTO.fromTask(res.locals.task));
};

export const taskDecision = async (req: Request, res: Response, next: NextFunction) => {
  const taskService = new TaskService();
  const task = res.locals.task;
  const dataset = task.dataset;
  const user = req.user! as User;

  if (!task.open) {
    next(new BadRequestException('errors.task_not_open'));
    return;
  }

  if (!isApproverForDataset(user, dataset)) {
    logger.warn(`User ${user.id} is not an approver for dataset ${dataset.id}`);
    next(new ForbiddenException('errors.user_is_not_approver_for_this_dataset'));
    return;
  }

  try {
    const dto = await dtoValidator(TaskDecisionDTO, req.body);

    // handle the decision
    if (task.action === TaskAction.Publish && task.status === TaskStatus.Requested) {
      if (dto.decision === 'approve') {
        await req.datasetService.approvePublication(dataset.id, dataset.draftRevisionId, user, req.fileService);
      }
      if (dto.decision === 'reject') {
        await req.datasetService.rejectPublication(dataset.id, dataset.draftRevisionId);
      }
    }

    // finally, update the task status
    const updatedTask = await taskService.decision(task.id, dto, user);
    res.json(TaskDTO.fromTask(updatedTask));
  } catch (err) {
    next(err);
  }
};
