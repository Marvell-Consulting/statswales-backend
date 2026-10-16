import { EntityManager, FindOptionsRelations, Repository } from 'typeorm';

import { logger } from '../utils/logger';
import { Task, TaskMetadata } from '../entities/task/task';
import { TaskAction } from '../enums/task-action';
import { TaskStatus } from '../enums/task-status';
import { User } from '../entities/user/user';
import { DatasetRepository } from '../repositories/dataset';
import { PublishedDatasetRepository } from '../repositories/published-dataset';
import { getPublishingStatus } from '../utils/dataset-status';
import { PublishingStatus } from '../enums/publishing-status';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { Locale } from '../enums/locale';
import { publisherDataSource } from '../db/publisher-source';
import { withTransaction } from '../db/transaction';

export class TaskService {
  // pass a transactional EntityManager to run the task writes as part of that transaction
  constructor(private readonly em: EntityManager = publisherDataSource.manager) {}

  private get tasks(): Repository<Task> {
    return this.em.getRepository(Task);
  }

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
    const task = this.tasks.create({ datasetId, action, createdBy, status, open, comment, metadata });

    return await this.tasks.save(task);
  }

  async getById(taskId: string, relations: FindOptionsRelations<Task> = {}): Promise<Task> {
    logger.info(`Getting task ${taskId}`);
    return await this.tasks.findOneOrFail({ where: { id: taskId }, relations });
  }

  async withdrawApproved(datasetId: string, revisionId: string, user: User): Promise<Task> {
    logger.info(`Withdrawing an approved but unpublished dataset`);

    // if the dataset was previously approved then the existing publish task was already closed, so we can't update it.
    // so instead create an extra closed withdraw task so that the event still appears in the dataset history
    const task = this.tasks.create({
      datasetId,
      action: TaskAction.Publish,
      status: TaskStatus.Withdrawn,
      open: false,
      metadata: { revisionId, note: 'previously approved' },
      createdBy: user,
      updatedBy: user
    });

    return this.tasks.save(task);
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

  async rejectUnpublish(taskId: string, user: User, reason: string): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    logger.info(`Rejecting unpublish for dataset ${task.dataset?.id}`);
    return this.update(task.id, TaskStatus.Rejected, false, user, reason);
  }

  async requestArchive(
    datasetId: string,
    user: User,
    reason: string,
    replacementDatasetId?: string,
    autoRedirect?: boolean
  ): Promise<void> {
    logger.info(`Requesting archive for dataset ${datasetId}`);
    const dataset = await DatasetRepository.getById(datasetId, { tasks: true });

    if (dataset.tasks?.some((task) => task.open)) {
      logger.warn(`Cannot request archive dataset ${datasetId} because it has open tasks`);
      throw new BadRequestException('errors.request_archive.open_tasks');
    }

    const metadata: TaskMetadata = { revisionId: dataset.endRevisionId };
    if (replacementDatasetId) {
      if (replacementDatasetId === datasetId) {
        throw new BadRequestException('errors.request_archive.replacement_is_self');
      }
      try {
        const replacement = await PublishedDatasetRepository.getById(replacementDatasetId, {
          publishedRevision: { metadata: true }
        });
        if (replacement.archivedAt) {
          throw new BadRequestException('errors.request_archive.replacement_archived');
        }
        const title = replacement.publishedRevision?.metadata?.find((m) =>
          m.language.toLowerCase().includes(Locale.English)
        )?.title;
        metadata.replacementDatasetId = replacementDatasetId;
        metadata.replacementDatasetTitle = title;
        metadata.autoRedirect = autoRedirect ?? false;
      } catch (err) {
        if (err instanceof BadRequestException) throw err;
        logger.error({ err }, `Replacement dataset ${replacementDatasetId} not found or not published`);
        throw new BadRequestException('errors.request_archive.replacement_not_published');
      }
    }

    await this.create(datasetId, TaskAction.Archive, user, reason, metadata);
  }

  async approveArchive(taskId: string, user: User): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    const dataset = task.dataset!;
    logger.info(`Approving archive for dataset ${dataset.id}`);

    const replacementDatasetId = task.metadata?.replacementDatasetId as string | undefined;
    const autoRedirect = task.metadata?.autoRedirect as boolean | undefined;

    return withTransaction(async (em) => {
      await em.withRepository(DatasetRepository).archive(dataset.id, replacementDatasetId, autoRedirect);
      return new TaskService(em).update(task.id, TaskStatus.Approved, false, user, null);
    }, this.em);
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

    return withTransaction(async (em) => {
      await em.withRepository(DatasetRepository).unarchive(dataset.id);
      return new TaskService(em).update(task.id, TaskStatus.Approved, false, user, null);
    }, this.em);
  }

  async rejectUnarchive(taskId: string, user: User, reason: string): Promise<Task> {
    const task = await this.getById(taskId, { dataset: true });
    logger.info(`Rejecting unarchive for dataset ${task.dataset?.id}`);
    return this.update(task.id, TaskStatus.Rejected, false, user, reason);
  }

  async closeOpenPublishTasks(datasetId: string, user: User, exceptTaskId?: string, tasks?: Task[]): Promise<void> {
    const candidates =
      tasks ??
      (await this.tasks.find({
        where: { datasetId, action: TaskAction.Publish, open: true },
        order: { createdAt: 'DESC' }
      }));

    const toClose = candidates.filter((task) => {
      const matchesDataset = task.datasetId === datasetId || task.dataset?.id === datasetId;
      return matchesDataset && task.action === TaskAction.Publish && task.open && task.id !== exceptTaskId;
    });

    if (toClose.length === 0) return;

    await withTransaction(async (em) => {
      const tasks = em.getRepository(Task);
      for (const task of toClose) {
        logger.info(`Closing open publish task ${task.id} for dataset ${datasetId}`);
        await tasks.save(tasks.merge(task, { status: TaskStatus.Withdrawn, open: false, updatedBy: user }));
      }
    }, this.em);
  }

  async update(taskId: string, status: TaskStatus, open: boolean, user: User, comment?: string | null): Promise<Task> {
    logger.debug(`Updating task ${taskId} with status ${status}`);
    const task = await this.tasks.findOneByOrFail({ id: taskId });

    return await this.tasks.save(this.tasks.merge(task, { status, open, updatedBy: user, comment }));
  }

  async getTasksForDataset(datasetId: string, open?: boolean): Promise<Task[]> {
    logger.info(`Getting tasks for dataset ${datasetId}`);

    return await this.tasks.find({
      where: { datasetId, open },
      order: { createdAt: 'DESC' }
    });
  }
}
