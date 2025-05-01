import { logger } from '../utils/logger';
import { Task } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';

type Entity = 'dataset' | 'revision';

export class TaskService {
  async create(entity: Entity, entityId: string, action: TaskAction, user: User, comment?: string): Promise<Task> {
    logger.info('Creating new task', { entity, action });

    const task = Task.create({
      action,
      status: TaskStatus.Requested,
      open: true,
      entity: entity,
      entityId: entityId,
      comment,
      submittedBy: user
    });

    return await task.save();
  }

  async getTasksForDataset(datasetId: string, open = true): Promise<Task[]> {
    logger.info('Getting tasks for dataset', { datasetId, open });

    const tasks = await Task.find({
      where: {
        entity: 'dataset',
        entityId: datasetId,
        open
      },
      order: {
        createdAt: 'DESC'
      }
    });

    return tasks;
  }
}
