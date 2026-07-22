import { In, JsonContains, QueryFailedError } from 'typeorm';

import { format as pgformat } from '@scaleleap/pg-format';
import { RevisionMetadataDTO } from '../dtos/revision-metadata-dto';
import { TranslationDTO } from '../dtos/translations-dto';
import { Dataset } from '../entities/dataset/dataset';
import { User } from '../entities/user/user';
import { Locale } from '../enums/locale';
import {
  DatasetRepository,
  withDraftAndMetadata,
  withDraftAndProviders,
  withDraftAndTopics
} from '../repositories/dataset';
import { RevisionRepository } from '../repositories/revision';
import { logger } from '../utils/logger';
import { DataTableAction } from '../enums/data-table-action';
import { RevisionProviderDTO } from '../dtos/revision-provider-dto';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { DimensionRepository } from '../repositories/dimension';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { isPublished } from '../utils/revision';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { TasklistStateDTO } from '../dtos/tasklist-state-dto';
import { EventLog } from '../entities/event-log';

import { createAllCubeFiles } from './cube-builder';
import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';
import { UnknownException } from '../exceptions/unknown.exception';
import { validateAndUpload } from './incoming-file-processor';
import { removeAllDimensions, removeMeasure } from './dimension-processor';
import { UserGroupRepository } from '../repositories/user-group';
import { TaskService } from './task';
import { TaskAction } from '../enums/task-action';
import { Task } from '../entities/task/task';
import { TaskStatus } from '../enums/task-status';
import { getPublishingStatus } from '../utils/dataset-status';
import { PublishingStatus as PubStatus } from '../enums/publishing-status';

import {
  omitDatasetUpdates,
  flagUpdateTask,
  generateSimulatedEvents,
  omitRevisionUpdates
} from '../utils/dataset-history';
import { StorageService } from '../interfaces/storage-service';
import { TempFile } from '../interfaces/temp-file';
import { dbManager } from '../db/database-manager';
import { bootstrapCubeBuildProcess } from '../utils/lookup-table-utils';

export class DatasetService {
  lang: Locale;
  taskService: TaskService;
  fileService: StorageService;

  constructor(lang: Locale, fileService: StorageService) {
    this.lang = lang;
    this.taskService = new TaskService();
    this.fileService = fileService;
  }

  async createNew(title: string, userGroupId: string, createdBy: User): Promise<Dataset> {
    logger.info(`Creating new dataset...`);

    const dataset = await DatasetRepository.create({ createdBy, userGroupId }).save();
    const firstRev = await RevisionRepository.create({ dataset, createdBy, revisionIndex: 1 }).save();
    await RevisionRepository.createMetadata(firstRev, title, this.lang);

    await DatasetRepository.save({
      ...dataset,
      draftRevision: firstRev,
      startRevision: firstRev,
      endRevision: firstRev
    });

    logger.info(`Dataset '${dataset.id}' created with draft revision '${firstRev.id}'`);

    return DatasetRepository.getById(dataset.id, withDraftAndMetadata);
  }

  async getDatasetOverview(datasetId: string): Promise<Dataset> {
    return DatasetRepository.getById(datasetId, {
      publishedRevision: { metadata: true },
      endRevision: { metadata: true },
      tasks: { createdBy: true, updatedBy: true },
      replacementDataset: { publishedRevision: { metadata: true } }
    });
  }

  async updateMetadata(datasetId: string, metadata: RevisionMetadataDTO): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, withDraftAndMetadata);
    await RevisionRepository.updateMetadata(dataset.draftRevision!, metadata);

    return DatasetRepository.getById(dataset.id, {});
  }

  async updateFactTable(datasetId: string, file: TempFile, userId?: string): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, {
      factTable: true,
      draftRevision: { dataTable: true }
    });

    const draftRevision = dataset.draftRevision;
    if (!draftRevision) {
      throw new BadRequestException('errors.update_fact_table.no_draft_revision');
    }

    if (draftRevision.revisionIndex !== 1) {
      throw new BadRequestException('errors.update_fact_table.not_first_revision');
    }

    logger.debug('Uploading new fact table file to filestore');
    const dataTable = await validateAndUpload(file, datasetId, 'data_table');

    dataTable.action = DataTableAction.ReplaceAll;

    dataTable.dataTableDescriptions.forEach((col) => {
      col.factTableColumn = col.columnName;
    });

    // the guard above already guarantees this is the first revision; keep the explicit check as a
    // defensive backstop in case that guard is ever relaxed, so we never reset a later revision
    if (draftRevision.revisionIndex === 1) {
      await removeAllDimensions(dataset);
      await removeMeasure(dataset);
    }

    await RevisionRepository.replaceDataTable(draftRevision, dataTable);
    await DatasetRepository.replaceFactTable(dataset, dataTable);
    await createAllCubeFiles(datasetId, draftRevision.id, userId);

    return DatasetRepository.getById(datasetId, {});
  }

  async addDataProvider(datasetId: string, dataProvider: RevisionProviderDTO): Promise<Dataset> {
    const newProvider = RevisionProviderDTO.toRevisionProvider(dataProvider);

    const altLang = newProvider.language.includes(Locale.English) ? Locale.WelshGb : Locale.EnglishGb;

    const newProviderAltLang: Partial<RevisionProvider> = {
      ...newProvider,
      id: undefined,
      language: altLang.toLowerCase()
    };

    await RevisionProvider.getRepository().save([newProvider, newProviderAltLang]);

    logger.debug(`Added new provider for dataset ${datasetId}`);

    return DatasetRepository.getById(datasetId, withDraftAndProviders);
  }

  async updateDataProviders(datasetId: string, dataProviders: RevisionProviderDTO[]): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, { draftRevision: { revisionProviders: true } });
    const existing = dataset.draftRevision!.revisionProviders;
    const submitted = dataProviders.map((provider) => RevisionProviderDTO.toRevisionProvider(provider));

    // work out what providers have been removed and remove for both languages
    const toRemove = existing.filter((existing) => {
      // if the group id is still present in the submitted data then don't remove those providers
      return !submitted.some((submitted) => submitted.groupId === existing.groupId);
    });

    await RevisionProvider.getRepository().remove(toRemove);

    // update the data providers for both languages
    const toUpdate = existing
      .filter((existing) => submitted.some((submitted) => submitted.groupId === existing.groupId))
      .map((updating) => {
        const updated = submitted.find((submitted) => submitted.groupId === updating.groupId)!;
        updating.providerId = updated.providerId;
        updating.providerSourceId = updated.providerSourceId;

        return updating;
      });

    await RevisionProvider.getRepository().save(toUpdate);

    logger.debug(
      `Removed ${toRemove.length} providers and updated ${toUpdate.length} providers for dataset ${datasetId}`
    );

    return DatasetRepository.getById(datasetId, withDraftAndProviders);
  }

  async updateTopics(datasetId: string, topics: string[]): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, { draftRevision: { revisionTopics: true } });
    const revision = dataset.draftRevision!;

    // remove any existing topic relations
    const existingTopics = revision.revisionTopics;
    await RevisionTopic.getRepository().remove(existingTopics);

    // save the new topic relations
    const newTopics = topics.map((topicId: string) => {
      return RevisionTopic.getRepository().create({ revisionId: revision.id, topicId: parseInt(topicId, 10) });
    });

    await RevisionTopic.getRepository().save(newTopics);

    return DatasetRepository.getById(datasetId, withDraftAndTopics);
  }

  async updateTranslations(datasetId: string, translations: TranslationDTO[]): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, {
      draftRevision: { metadata: true },
      dimensions: { metadata: true },
      measure: { metadata: true }
    });

    const revision = dataset.draftRevision!;
    const dimensions = dataset.dimensions;

    // set all metadata updated_at to the same time, we can use this later to flag untranslated changes
    const now = new Date();

    logger.debug(`Updating dimension name translations...`);
    dimensions.forEach((dimension) => {
      const translation = translations.find((t) => t.type === 'dimension' && t.key === dimension.factTableColumn)!;

      const dimMetaEN = dimension.metadata.find((meta) => meta.language.includes('en'))!;
      dimMetaEN.name = translation?.english || '';
      dimMetaEN.updatedAt = now;

      const dimMetaCY = dimension.metadata.find((meta) => meta.language.includes('cy'))!;
      dimMetaCY.name = translation?.cymraeg || '';
      dimMetaCY.updatedAt = now;
    });

    await DimensionRepository.save(dimensions);

    logger.debug(`Updating metadata translations...`);
    const metaTranslations = translations.filter((t) => t.type === 'metadata');
    const metaEn = revision.metadata.find((meta) => meta.language === Locale.EnglishGb)!;
    const metaCy = revision.metadata.find((meta) => meta.language === Locale.WelshGb)!;

    metaTranslations.forEach((row) => {
      const metaKey = row.key as 'title' | 'summary' | 'collection' | 'quality' | 'roundingDescription';
      metaEn[metaKey] = row.english || '';
      metaCy[metaKey] = row.cymraeg || '';
    });

    metaEn.updatedAt = now;
    metaCy.updatedAt = now;

    await RevisionMetadata.getRepository().save([metaEn, metaCy]);

    logger.debug(`Updating related link translations...`);
    revision.relatedLinks?.forEach((link) => {
      const translation = translations.find((t) => t.type === 'link' && t.key === link.id);
      link.labelEN = translation?.english;
      link.labelCY = translation?.cymraeg;
    });

    await RevisionRepository.save(revision);

    return DatasetRepository.getById(datasetId, {});
  }

  async submitForPublication(datasetId: string, revisionId: string, user: User): Promise<void> {
    const dataset = await DatasetRepository.getById(datasetId, { draftRevision: true });

    if (!dataset.draftRevision || dataset.draftRevision.id !== revisionId) {
      throw new BadRequestException('errors.submit_for_publication.invalid_revision_id');
    }

    const openTasks = await this.getOpenTasks(datasetId);
    const rejectedPublishTask = openTasks.find(
      (task) => task.action === TaskAction.Publish && task.status === TaskStatus.Rejected
    );

    if (rejectedPublishTask) {
      const comment = null; // clear the rejection comment
      await this.taskService.update(rejectedPublishTask.id, TaskStatus.Requested, true, user, comment);
      return; // resubmission of a rejected task
    }

    const pendingPublishTask = openTasks.find(
      (task) => task.action === TaskAction.Publish && task.status === TaskStatus.Requested
    );

    if (pendingPublishTask) {
      // already awaiting approval — treat a duplicate submission (e.g. a double click) as a no-op
      // rather than creating a second open publish task
      logger.info(`Dataset ${datasetId} already has a pending publish task; ignoring duplicate submission`);
      return;
    }

    try {
      await this.taskService.create(datasetId, TaskAction.Publish, user, undefined, { revisionId });
    } catch (err) {
      // Backstop for the concurrent-submit race: a partial unique index guarantees at most one
      // open publish task per dataset, so a duplicate insert means another request won the race.
      const pg =
        err instanceof QueryFailedError ? (err.driverError as { code?: string; constraint?: string }) : undefined;
      if (pg?.code === '23505' && pg?.constraint === 'UQ_task_one_open_publish_per_dataset') {
        logger.warn(`Concurrent publish submission detected for dataset ${datasetId}; ignoring duplicate`);
        return;
      }
      throw err;
    }
  }

  async withdrawFromPublication(datasetId: string, revisionId: string, user: User): Promise<void> {
    const dataset = await DatasetRepository.getById(datasetId, { endRevision: true, tasks: true });
    const publishingStatus = getPublishingStatus(dataset, dataset.endRevision!);

    if (
      ![
        PubStatus.PendingApproval,
        PubStatus.UpdatePendingApproval,
        PubStatus.Scheduled,
        PubStatus.UpdateScheduled
      ].includes(publishingStatus)
    ) {
      throw new BadRequestException('errors.withdraw.no_pending_publication');
    }

    const draftRevision = await RevisionRepository.revertToDraft(revisionId);

    if (draftRevision.onlineCubeFilename) {
      await this.fileService.delete(draftRevision.onlineCubeFilename, datasetId);
    }

    // dataset.tasks was loaded above; derive open publish tasks from it to avoid a second DB read
    const openPublishTasks = (dataset.tasks ?? []).filter((task) => task.action === TaskAction.Publish && task.open);

    if (openPublishTasks.length > 0) {
      // close every open publish task (there should only be one, but close any duplicates too)
      await this.taskService.closeOpenPublishTasks(datasetId, user, undefined, openPublishTasks);
    } else {
      // the dataset was previously approved so its publish task is already closed; record a
      // closed withdraw task so the event still appears in the dataset history
      await this.taskService.withdrawApproved(datasetId, draftRevision.id, user);
    }
  }

  async approvePublication(datasetId: string, revisionId: string, user: User): Promise<Dataset> {
    const start = performance.now();
    // Open the build log before bootstrap so a failure in either the bootstrap or
    // the build proper is recorded in build_log (bootstrap previously ran before any
    // build_log row existed, so its failures were only visible in application logs).
    const build = await BuildLog.startBuild({ id: revisionId }, CubeBuildType.FullCube, user.id);
    try {
      await bootstrapCubeBuildProcess(datasetId, revisionId);
      await createAllCubeFiles(datasetId, revisionId, user.id, CubeBuildType.FullCube, build, true);
    } catch (err) {
      // createAllCubeFiles records and saves its own failure; only mark it here when the
      // failure happened earlier (e.g. during bootstrap), so we don't clobber that detail.
      if (build.status !== CubeBuildStatus.Failed && build.status !== CubeBuildStatus.Completed) {
        const message = err instanceof Error ? err.message : String(err);
        build.completeBuild(CubeBuildStatus.Failed, undefined, JSON.stringify({ message }));
        await build.save();
      }
      logger.error(err, 'approvePublication: cube build failed during approval');
      // Surface a clean, translated message instead of leaking the raw Postgres error.
      throw new UnknownException('errors.cube_builder.cube_build_failed');
    }
    const scheduledRevision = await RevisionRepository.approvePublication(revisionId, `${revisionId}.duckdb`, user);
    const approvedDataset = await DatasetRepository.publish(scheduledRevision);
    const time = Math.round(performance.now() - start);
    logger.info(`Publication approved, time: ${time}ms`);

    return approvedDataset;
  }

  async rejectPublication(datasetId: string, revisionId: string): Promise<void> {
    const revision = await RevisionRepository.revertToDraft(revisionId);

    if (revision.onlineCubeFilename) {
      await this.fileService.delete(revision.onlineCubeFilename, datasetId);
    }
  }

  async createRevision(datasetId: string, createdBy: User): Promise<Dataset> {
    logger.info(`Creating new revision for dataset: ${datasetId}...`);

    const dataset = await DatasetRepository.findOneOrFail({
      where: { id: datasetId },
      relations: { publishedRevision: true, revisions: true }
    });

    if (dataset.revisions.some((rev) => !isPublished(rev))) {
      throw new BadRequestException('errors.create_revision.existing_draft_revision');
    }

    const publishedRevision = dataset.publishedRevision!;
    const newRevision = await RevisionRepository.deepCloneRevision(publishedRevision.id, createdBy);
    logger.info(`New draft revision created: ${newRevision.id}`);

    await DatasetRepository.save({ id: datasetId, draftRevision: newRevision, endRevision: newRevision });

    return DatasetRepository.getById(datasetId, withDraftAndMetadata);
  }

  async deleteDraftRevision(datasetId: string, revisionId: string): Promise<void> {
    const dataset = await DatasetRepository.getById(datasetId, {
      draftRevision: { dataTable: true, previousRevision: true }
    });

    const draft = dataset.draftRevision;

    if (!draft || draft.id !== revisionId) {
      throw new BadRequestException('errors.delete_draft_revision.no_draft_revision');
    }

    const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
    try {
      logger.warn(`Deleting draft revision ${revisionId} from cube database and data lake...`);
      await cubeDB.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE', revisionId));
      if (draft.dataTable?.id) {
        await cubeDB.query(pgformat('DROP TABLE IF EXISTS data_tables.%I;', draft.dataTable?.id));
        await this.fileService.delete(draft.dataTable.id, datasetId);
      }
    } catch (err) {
      logger.warn(
        err,
        `Something went wrong trying to clean up cube database and data lake for revision ${revisionId}`
      );
    } finally {
      await cubeDB.release();
    }

    dataset.draftRevision = null;
    dataset.endRevision = draft.previousRevision;
    await dataset.save();
    await draft.remove();
  }

  async getTasklistState(datasetId: string, locale: Locale): Promise<TasklistStateDTO> {
    logger.debug(`Generating tasklist state for: ${datasetId}`);

    const dataset = await DatasetRepository.getById(datasetId, {
      dimensions: { metadata: true },
      measure: { metadata: true }
    });

    const revision = await RevisionRepository.getById(dataset.draftRevisionId!, {
      metadata: true,
      dataTable: true,
      revisionProviders: true,
      revisionTopics: true
    });

    if (!revision) {
      throw new BadRequestException('errors.get_tasklist_state.no_draft_revision');
    }

    dataset.draftRevision = revision;

    if (revision.previousRevisionId) {
      const previousRevision = await RevisionRepository.getById(revision.previousRevisionId, {
        metadata: true,
        dataTable: true,
        revisionProviders: true,
        revisionTopics: true
      });

      revision.previousRevision = previousRevision;
    }

    const translationEvents = await EventLog.getRepository().find({
      where: { entity: 'translations', entityId: revision.id },
      order: { createdAt: 'DESC' }
    });

    logger.debug(`Found ${translationEvents.length} translation events for revision: ${revision.id}`);

    return TasklistStateDTO.fromDataset(dataset, revision, locale, translationEvents);
  }

  async updateDatasetGroup(datasetId: string, userGroupId: string): Promise<Dataset> {
    const dataset = await DatasetRepository.findOneByOrFail({ id: datasetId });
    const userGroup = await UserGroupRepository.findOneByOrFail({ id: userGroupId });
    dataset.userGroupId = userGroup.id;
    return dataset.save();
  }

  async getOpenTasks(datasetId: string): Promise<Task[]> {
    return this.taskService.getTasksForDataset(datasetId, true);
  }

  async getAllTasks(datasetId: string): Promise<Task[]> {
    return this.taskService.getTasksForDataset(datasetId);
  }

  async getPendingPublishTask(datasetId: string): Promise<Task | undefined> {
    return (await this.getOpenTasks(datasetId)).find(
      (task) => task.action === TaskAction.Publish && task.status === TaskStatus.Requested
    );
  }

  async getRejectedPublishTask(datasetId: string): Promise<Task | undefined> {
    return (await this.getOpenTasks(datasetId)).find(
      (task) => task.action === TaskAction.Publish && task.status === TaskStatus.Rejected
    );
  }

  async getHistory(datasetId: string): Promise<EventLog[]> {
    const dataset = await DatasetRepository.getById(datasetId, { revisions: true });
    const revisionIds = dataset.revisions.map((rev) => rev.id);

    const history = await EventLog.find({
      where: [
        { entity: 'dataset', entityId: datasetId },
        { entity: 'revision', entityId: In(revisionIds) },
        { entity: 'task', data: JsonContains({ datasetId }) }
      ],
      order: { createdAt: 'DESC' },
      relations: { user: true }
    });

    history.push(...generateSimulatedEvents(dataset));
    history.sort((a, b) => (a.createdAt > b.createdAt ? -1 : 1)); // resort desc for generated events

    return history
      .filter(omitDatasetUpdates)
      .filter(omitRevisionUpdates)
      .map((event) => flagUpdateTask(dataset, event));
  }

  async approveUnpublish(datasetId: string, user: User): Promise<void> {
    logger.info(`Unpublishing dataset ${datasetId}`);

    let dataset = await DatasetRepository.getById(datasetId, { publishedRevision: true });

    if (!dataset.publishedRevision) {
      throw new Error(`Dataset ${datasetId} does not have a published revision`);
    }

    // mark the current published revision as unpublished
    dataset.publishedRevision.unpublishedAt = new Date();
    await RevisionRepository.save(dataset.publishedRevision);
    logger.info(`Revision ${dataset.publishedRevision.id} marked as unpublished`);

    // create a new draft revision based on the now unpublished revision
    dataset = await this.createRevision(datasetId, user);
    await createAllCubeFiles(dataset.id, dataset.draftRevision!.id, user.id);
  }
}
