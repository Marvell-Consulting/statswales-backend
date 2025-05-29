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

  async withdrawPending(taskId: string, user: User): Promise<Task> {
    logger.info(`Withdrawing pending task ${taskId}`);
    const task = await Task.findOneByOrFail({ id: taskId });
    const updatedTask = Task.merge(task, { status: TaskStatus.Withdrawn, open: false, updatedBy: user });
    return await updatedTask.save();
  }

  async withdrawApproved(datasetId: string, revisionId: string, user: User): Promise<Task> {
    logger.info(`Withdrawing an approved but unpublished dataset`);

    // if the dataset was previously approved then the existing publish task was already closed, so we can't update it.
    // so instead create an extra closed withdraw task so that the event still appears in the dataset history
    const task = Task.create({
      datasetId,
      action: TaskAction.Publish,
      status: TaskStatus.Withdrawn,
      open: false,
      metadata: { revisionId, note: 'previously approved' },
      createdBy: user,
      updatedBy: user
    });

    return task.save();
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

  async update(taskId: string, status: TaskStatus, open: boolean, user: User, comment?: string | null): Promise<Task> {
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
