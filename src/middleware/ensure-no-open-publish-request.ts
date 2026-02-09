import { Request, Response, NextFunction } from 'express';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { TaskService } from '../services/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';

export const ensureNoOpenPublishRequest = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  const dataset = res.locals.dataset;
  const taskService = new TaskService();

  try {
    const openTasks = await taskService.getTasksForDataset(dataset.id, true);

    if (openTasks.some((task) => task.action === TaskAction.Publish && task.status === TaskStatus.Requested)) {
      return next(new BadRequestException('errors.dataset.has_open_publish_request'));
    }
  } catch (err) {
    return next(err);
  }

  next();
};
