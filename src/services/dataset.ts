import { FindOptionsRelations, In, JsonContains } from 'typeorm';

import { format as pgformat } from '@scaleleap/pg-format';
import { RevisionMetadataDTO } from '../dtos/revistion-metadata-dto';
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
import { getFileService } from '../utils/get-file-service';
import { DimensionType } from '../enums/dimension-type';
import { DateExtractor } from '../extractors/date-extractor';

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
      tasks: { createdBy: true, updatedBy: true }
    });
  }

  async updateMetadata(datasetId: string, metadata: RevisionMetadataDTO): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, withDraftAndMetadata);
    await RevisionRepository.updateMetadata(dataset.draftRevision!, metadata);

    return DatasetRepository.getById(dataset.id, {});
  }

  async updateFactTable(datasetId: string, file: TempFile): Promise<Dataset> {
    const dataset = await DatasetRepository.getById(datasetId, {
      factTable: true,
      draftRevision: { dataTable: true }
    });

    logger.debug('Uploading new fact table file to filestore');
    const dataTable = await validateAndUpload(file, datasetId, 'data_table');

    dataTable.action = DataTableAction.ReplaceAll;

    dataTable.dataTableDescriptions.forEach((col) => {
      col.factTableColumn = col.columnName;
    });

    if (dataset.draftRevision?.revisionIndex === 1) {
      await removeAllDimensions(dataset);
      await removeMeasure(dataset);
    }

    await RevisionRepository.replaceDataTable(dataset.draftRevision!, dataTable);
    await DatasetRepository.replaceFactTable(dataset, dataTable);
    await createAllCubeFiles(datasetId, dataset.draftRevision!.id);

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

    const rejectedPublishTask = await this.getRejectedPublishTask(datasetId);

    if (rejectedPublishTask) {
      const comment = null; // clear the rejection comment
      await this.taskService.update(rejectedPublishTask.id, TaskStatus.Requested, true, user, comment);
      return; // resubmission of a rejected task
    }

    await this.taskService.create(datasetId, TaskAction.Publish, user, undefined, { revisionId });
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

    const pendingPublicationTask = await this.getPendingPublishTask(datasetId);

    if (pendingPublicationTask) {
      await this.taskService.withdrawPending(pendingPublicationTask.id, user);
    } else {
      await this.taskService.withdrawApproved(datasetId, draftRevision.id, user);
    }
  }

  async approvePublication(datasetId: string, revisionId: string, user: User): Promise<Dataset> {
    const start = performance.now();
    await createAllCubeFiles(datasetId, revisionId);
    const datasetRelations: FindOptionsRelations<Dataset> = {
      dimensions: true
    };
    const datasetWithDimensions = await DatasetRepository.getById(datasetId, datasetRelations);
    let startDate: Date | null = null;
    let endDate: Date | null = null;
    datasetWithDimensions.dimensions
      .filter((dim) => dim.type === DimensionType.DatePeriod || dim.type === DimensionType.Date)
      .forEach((dim) => {
        const extractor = dim.extractor as DateExtractor;
        if (extractor.lookupTableStart) {
          if (!startDate) {
            startDate = extractor.lookupTableStart;
          } else if (extractor.lookupTableStart < startDate) {
            startDate = extractor.lookupTableStart;
          }
        }
        if (extractor.lookupTableEnd) {
          if (!endDate) {
            endDate = extractor.lookupTableEnd;
          } else if (extractor.lookupTableEnd > endDate) {
            endDate = extractor.lookupTableEnd;
          }
        }
      });
    const scheduledRevision = await RevisionRepository.approvePublication(revisionId, `${revisionId}.duckdb`, user);
    const approvedDataset = await DatasetRepository.publish(scheduledRevision, startDate, endDate);
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
      logger.error(`Dataset does not have a draft revision or the revision id does not match the current draft`);
      throw new BadRequestException('errors.delete_draft_revision.no_draft_revision');
    }

    const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
    const fileService = getFileService();
    try {
      logger.warn(`Deleting draft revision ${revisionId} from cube database and data lake...`);
      await cubeDB.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE', revisionId));
      if (draft.dataTable?.id) {
        await cubeDB.query(pgformat('DROP TABLE IF EXISTS data_tables.%I;', draft.dataTable?.id));
        await fileService.delete(draft.dataTable.id, datasetId);
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
    await dataset.publishedRevision.save();
    logger.info(`Revision ${dataset.publishedRevision.id} marked as unpublished`);

    // create a new draft revision based on the now unpublished revision
    dataset = await this.createRevision(datasetId, user);
    await createAllCubeFiles(dataset.id, dataset.draftRevision!.id);
  }
}
