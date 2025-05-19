import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import { Task, TaskMetadata } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';
import { TaskDecisionDTO } from '../dtos/task-decision-dto';

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

  async getById(taskId: string, relations: FindOptionsRelations<Task> = {}): Promise<Task> {
    logger.info(`Getting task ${taskId}`);
    return await Task.findOneOrFail({ where: { id: taskId }, relations });
  }

  async withdraw(taskId: string, user: User): Promise<Task> {
    logger.info(`Withdrawing task ${taskId}`);
    const task = await Task.findOneByOrFail({ id: taskId });
    const updatedTask = Task.merge(task, { status: TaskStatus.Withdrawn, open: false, updatedBy: user });
    return await updatedTask.save();
  }

  async decision(taskId: string, decision: TaskDecisionDTO, user: User): Promise<Task> {
    logger.info(`Decision received for task ${taskId}: ${decision.decision}`);

    if (decision.decision === 'approve') {
      // task is resolved and closed
      return await this.update(taskId, TaskStatus.Approved, false, user);
    }

    if (decision.decision === 'reject') {
      // leave task open so it can be re-submitted
      return await this.update(taskId, TaskStatus.Rejected, true, user, decision.reason);
    }

    return this.getById(taskId);
  }

  async update(taskId: string, status: TaskStatus, open: boolean, user: User, comment?: string): Promise<Task> {
    logger.info(`Updating task ${taskId} with status ${status}`);
    const task = await Task.findOneByOrFail({ id: taskId });
    const updatedTask = Task.merge(task, { status, open, updatedBy: user, comment });

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
