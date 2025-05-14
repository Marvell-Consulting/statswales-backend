import { logger } from '../utils/logger';
import { Task, TaskMetadata } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';

export class TaskService {
  async create(
    datasetId: string,
    action: TaskAction,
    createdBy: User,
    comment?: string,
    metadata?: TaskMetadata
  ): Promise<Task> {
    logger.info(`Creating ${action} task for dataset ${datasetId}`);

    const status = TaskStatus.Requested;
    const open = true;
    const task = Task.create({ datasetId, action, createdBy, status, open, comment, metadata });

    return await task.save();
  }

  async withdraw(taskId: string, user: User): Promise<Task> {
    logger.info(`Withdrawing task ${taskId}`);
    const task = await Task.findOneByOrFail({ id: taskId });
    const updatedTask = Task.merge(task, { status: TaskStatus.Withdrawn, open: false, updatedBy: user });
    return await updatedTask.save();
  }

  async resolve(taskId: string, status: TaskStatus, open: boolean, user: User): Promise<Task> {
    logger.info(`Resolving task ${taskId} with status ${status}`);
    const task = await Task.findOneByOrFail({ id: taskId });
    const updatedTask = Task.merge(task, { status, open, updatedBy: user });

    return await updatedTask.save();
  }

  async getTasksForDataset(datasetId: string, open?: boolean): Promise<Task[]> {
    logger.info(`Getting tasks for dataset ${datasetId}`);

    return await Task.find({
      where: { datasetId, open },
      order: { createdAt: 'DESC' }
    });
  }
}
