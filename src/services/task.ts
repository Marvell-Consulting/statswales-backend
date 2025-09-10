import { FindOptionsRelations } from 'typeorm';

import { logger } from '../utils/logger';
import { Task, TaskMetadata } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';
import { DatasetRepository } from '../repositories/dataset';
import { getPublishingStatus } from '../utils/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';
import { BadRequestException } from '../exceptions/bad-request.exception';

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

  async requestUnpublish(datasetId: string, user: User, reason: string): Promise<Task> {
    logger.info(`Requesting unpublish for dataset ${datasetId}`);
    const dataset = await DatasetRepository.getById(datasetId, { endRevision: true, tasks: true });

    if (dataset.tasks?.some((task) => task.open)) {
      logger.warn(`Cannot request unpublish dataset ${datasetId} because it has open tasks`);
      throw new BadRequestException('errors.request_unpublish.open_tasks');
    }

    const publishingStatus = getPublishingStatus(dataset, dataset.endRevision!);

    if (publishingStatus !== PublishingStatus.Published) {
      logger.warn(`Cannot unpublish dataset ${datasetId} because it is not in a published state: ${publishingStatus}`);
      throw new BadRequestException('errors.submit_for_unpublish.invalid_status');
    }

    return await this.create(datasetId, TaskAction.Unpublish, user, reason, { revisionId: dataset.endRevisionId });
  }

  async approveUnpublish(taskId: string, user: User): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    const dataset = task.dataset!;
    logger.info(`Approving unpublish for dataset ${dataset.id}`);

    await DatasetRepository.unpublish(dataset.id);
    return this.update(task.id, TaskStatus.Approved, false, user, null);
  }

  async rejectUnpublish(taskId: string, user: User, reason: string): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    logger.info(`Rejecting unpublish for dataset ${task.dataset?.id}`);
    return this.update(task.id, TaskStatus.Rejected, false, user, reason);
  }

  async requestArchive(datasetId: string, user: User, reason: string): Promise<void> {
    logger.info(`Requesting archive for dataset ${datasetId}`);
    const dataset = await DatasetRepository.getById(datasetId, { tasks: true });

    if (dataset.tasks?.some((task) => task.open)) {
      logger.warn(`Cannot request archive dataset ${datasetId} because it has open tasks`);
      throw new BadRequestException('errors.request_archive.open_tasks');
    }

    await this.create(datasetId, TaskAction.Archive, user, reason, { revisionId: dataset.endRevisionId });
  }

  async approveArchive(taskId: string, user: User): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    const dataset = task.dataset!;
    logger.info(`Approving archive for dataset ${dataset.id}`);

    await DatasetRepository.archive(dataset.id);
    return this.update(task.id, TaskStatus.Approved, false, user, null);
  }

  async rejectArchive(taskId: string, user: User, reason: string): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    logger.info(`Rejecting archive for dataset ${task.dataset?.id}`);
    return this.update(task.id, TaskStatus.Rejected, false, user, reason);
  }

  async requestUnarchive(datasetId: string, user: User, reason: string): Promise<void> {
    const dataset = await DatasetRepository.getById(datasetId, { tasks: true });

    if (dataset.tasks?.some((task) => task.open)) {
      logger.warn(`Cannot request unarchive dataset ${datasetId} because it has open tasks`);
      throw new BadRequestException('errors.request_unarchive.open_tasks');
    }

    await this.create(datasetId, TaskAction.Unarchive, user, reason, { revisionId: dataset.endRevisionId });
  }

  async approveUnarchive(taskId: string, user: User): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    const dataset = task.dataset!;
    logger.info(`Approving unarchive for dataset ${dataset.id}`);

    await DatasetRepository.unarchive(dataset.id);
    return this.update(task.id, TaskStatus.Approved, false, user, null);
  }

  async rejectUnarchive(taskId: string, user: User, reason: string): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    logger.info(`Rejecting unarchive for dataset ${task.dataset?.id}`);
    return this.update(task.id, TaskStatus.Rejected, false, user, reason);
  }

  async update(taskId: string, status: TaskStatus, open: boolean, user: User, comment?: string | null): Promise<Task> {
    logger.debug(`Updating task ${taskId} with status ${status}`);
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
