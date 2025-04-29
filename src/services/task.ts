import { logger } from '../utils/logger';
import { Task } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';
import { Revision } from '../entities/dataset/revision';
import { Dataset } from '../entities/dataset/dataset';

export class TaskService {
  async create(entity: Dataset | Revision, action: TaskAction, user: User, comment?: string): Promise<Task> {
    logger.info('Creating new task', { entity, action });
    const task = Task.create({
      action,
      status: TaskStatus.Requested,
      entity: entity.constructor.name,
      entityId: entity.id,
      comment,
      submittedBy: user
    });

    return await task.save();
  }
}
