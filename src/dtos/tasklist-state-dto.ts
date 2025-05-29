import { every, isEqual, max, pick, sortBy } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { TaskListStatus } from '../enums/task-list-status';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { DimensionStatus } from '../interfaces/dimension-status';
import { Revision } from '../entities/dataset/revision';
import { EventLog } from '../entities/event-log';
import { logger } from '../utils/logger';
import { TranslationDTO } from './translations-dto';
import { collectTranslations } from '../utils/collect-translations';

export interface MetadataStatus {
  title: TaskListStatus;
  summary: TaskListStatus;
  quality: TaskListStatus;
  collection: TaskListStatus;
  frequency: TaskListStatus;
  designation: TaskListStatus;
  related: TaskListStatus;
  sources: TaskListStatus;
  topics: TaskListStatus;
}

export interface TranslationStatus {
  export: TaskListStatus;
  import: TaskListStatus;
}

export interface PublishingStatus {
  when: TaskListStatus;
}

export class TasklistStateDTO {
  datatable: TaskListStatus;
  measure?: DimensionStatus;
  dimensions: DimensionStatus[];

  metadata: MetadataStatus;
  translation: TranslationStatus;
  publishing: PublishingStatus;

  canPublish: boolean;
  isUpdate: boolean;

  public static dataTableStatus(revision: Revision) {
    const isUpdate = Boolean(revision.previousRevisionId);

    if (isUpdate) {
      const uploadedAt = revision.dataTable?.uploadedAt;
      return uploadedAt && uploadedAt > revision.createdAt ? TaskListStatus.Updated : TaskListStatus.Unchanged;
    }

    return revision?.dataTable ? TaskListStatus.Completed : TaskListStatus.NotStarted;
  }

  public static measureStatus(dataset: Dataset, revision: Revision, lang: string) {
    if (!dataset.measure) return undefined;

    const measure = dataset.measure;
    const isUpdate = Boolean(revision.previousRevisionId);
    let status = TaskListStatus.NotStarted;

    if (isUpdate) {
      status = TaskListStatus.Unchanged;
    } else if (measure.joinColumn) {
      status = TaskListStatus.Completed;
    }

    const name = measure.metadata?.find((meta) => meta.language.includes(lang))?.name || measure.factTableColumn;

    return { type: 'measure', id: measure.id, name, status };
  }

  public static dimensionStatus(dataset: Dataset, revision: Revision, lang: string): DimensionStatus[] {
    const isUpdate = Boolean(revision.previousRevisionId);

    return dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
      if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

      const name = dimension.metadata.find((meta) => lang.includes(meta.language))?.name || dimension.factTableColumn;
      let status: TaskListStatus;

      if (isUpdate) {
        status = TaskListStatus.Unchanged;
        const updateTask = revision.tasks?.dimensions.find((task) => task.id === dimension.id);

        if (updateTask) {
          status = updateTask.lookupTableUpdated ? TaskListStatus.Updated : TaskListStatus.Unchanged;
        }
      } else {
        status = dimension.extractor === null ? TaskListStatus.NotStarted : TaskListStatus.Completed;
      }

      dimensionStatus.push({ name, status, id: dimension.id, type: dimension.type });

      return dimensionStatus;
    }, []);
  }

  public static metadataStatus(revision: Revision, lang: string): MetadataStatus {
    const metadata = revision?.metadata.find((meta) => lang.includes(meta.language));

    if (!metadata) {
      throw new Error(`Cannot generate tasklist state - metadata not found for language ${lang}`);
    }

    const isUpdate = Boolean(revision.previousRevisionId);

    if (isUpdate) {
      const prevRevision = revision.previousRevision;
      const prevMeta = prevRevision?.metadata.find((meta) => lang.includes(meta.language));

      if (!prevRevision || !prevMeta) {
        throw new Error(`Cannot generate tasklist state - previous metadata not found for language ${lang}`);
      }

      const topics = revision.revisionTopics.map((t) => t.topicId).sort();
      const prevTopics = prevRevision.revisionTopics.map((t) => t.topicId).sort();

      const providers = sortBy(
        revision.revisionProviders.map((prov) => pick(prov, ['providerId', 'providerSourceId', 'language'])),
        'language'
      );
      const prevProviders = sortBy(
        prevRevision.revisionProviders.map((prov) => pick(prov, ['providerId', 'providerSourceId', 'language'])),
        'language'
      );

      return {
        title: isEqual(prevMeta.title, metadata.title) ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        summary: isEqual(prevMeta.summary, metadata.summary) ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        quality: isEqual(prevMeta.quality, metadata.quality) ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        collection: isEqual(prevMeta.collection, metadata.collection)
          ? TaskListStatus.Unchanged
          : TaskListStatus.Updated,
        frequency: isEqual(prevRevision.updateFrequency, revision.updateFrequency)
          ? TaskListStatus.Unchanged
          : TaskListStatus.Updated,
        designation:
          prevRevision.designation === revision.designation ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        sources: isEqual(providers, prevProviders) ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        topics: isEqual(topics, prevTopics) ? TaskListStatus.Unchanged : TaskListStatus.Updated,
        related: isEqual(prevRevision.relatedLinks, revision.relatedLinks)
          ? TaskListStatus.Unchanged
          : TaskListStatus.Updated
      };
    }

    return {
      title: metadata?.title ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      summary: metadata?.summary ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      quality: metadata?.quality ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      collection: metadata?.collection ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      frequency: revision.updateFrequency ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      designation: revision?.designation ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      sources: revision?.revisionProviders?.length > 0 ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      topics: revision?.revisionTopics?.length > 0 ? TaskListStatus.Completed : TaskListStatus.NotStarted,
      related:
        revision?.relatedLinks && revision.relatedLinks?.length > 0
          ? TaskListStatus.Completed
          : TaskListStatus.NotStarted
    };
  }

  public static publishingStatus(dataset: Dataset, revision: Revision): PublishingStatus {
    return {
      when: revision.publishAt ? TaskListStatus.Completed : TaskListStatus.NotStarted
    };
  }

  public static translationStatus(
    dataset: Dataset,
    revision: Revision,
    translationEvents?: EventLog[]
  ): TranslationStatus {
    const isUpdate = Boolean(revision.previousRevisionId);

    if (isUpdate && revision.previousRevision) {
      const newTranslations = collectTranslations(dataset);
      const previousTranslations = collectTranslations(dataset, false, revision.previousRevision);

      // Compare draft revision with previous version
      if (isEqual(newTranslations, previousTranslations)) {
        return {
          import: TaskListStatus.Unchanged,
          export: TaskListStatus.Unchanged
        };
      }
    }

    const lastExportedAt = translationEvents?.find((event) => event.action === 'export')?.createdAt;
    const lastImportedAt = translationEvents?.find((event) => event.action === 'import')?.createdAt;

    const metaEN = revision.metadata?.find((meta) => meta.language.includes('en'));
    const metaCY = revision.metadata?.find((meta) => meta.language.includes('cy'));

    if (!metaEN || !metaCY) {
      throw new Error(`Cannot generate tasklist state - metadata missing`);
    }

    const metadataSynced = metaEN.updatedAt === metaCY.updatedAt;
    const lastMetaUpdateAt = max([metaEN.updatedAt, metaCY.updatedAt])!;

    const metaFullyTranslated = revision.metadata?.every((meta) => {
      return every(translatableMetadataKeys, (key) => {
        // ignore roundingDescription if rounding isn't applied, otherwise check some data exists
        return key === 'roundingDescription' && !revision.roundingApplied ? true : Boolean(meta[key]);
      });
    });

    const relatedLinksTranslated = every(revision.relatedLinks, (link) => {
      return link.labelEN && link.labelCY;
    });

    const lastExport = translationEvents?.find((event) => event.action === 'export');

    const existingTranslations = collectTranslations(dataset);

    // previously exported revisions did not include data, ignore these.
    const exportStale = lastExport?.data?.translations.some((incoming: TranslationDTO) => {
      const expected = existingTranslations.find((existing) => existing.key === incoming.key)?.english;
      // previous export value has since been updated
      return expected !== incoming.english;
    });

    const translationRequired = !metadataSynced || !metaFullyTranslated;

    let exportStatus: TaskListStatus;
    if (lastExportedAt) {
      exportStatus = exportStale ? TaskListStatus.Incomplete : TaskListStatus.Completed;
    } else {
      exportStatus = TaskListStatus.NotStarted;
    }

    let importStatus: TaskListStatus;
    if (lastImportedAt && lastImportedAt > lastMetaUpdateAt) {
      importStatus = exportStale || !relatedLinksTranslated ? TaskListStatus.Incomplete : TaskListStatus.Completed;
    } else {
      importStatus = TaskListStatus.NotStarted;
    }

    return {
      export: translationRequired ? exportStatus : TaskListStatus.NotRequired,
      import: translationRequired ? importStatus : TaskListStatus.NotRequired
    };
  }

  public static fromDataset(
    dataset: Dataset,
    revision: Revision,
    lang: string,
    translationEvents?: EventLog[]
  ): TasklistStateDTO {
    const isUpdate = Boolean(revision.previousRevisionId);

    const dto = new TasklistStateDTO();
    dto.isUpdate = isUpdate;

    dto.datatable = TasklistStateDTO.dataTableStatus(revision);
    dto.measure = TasklistStateDTO.measureStatus(dataset, revision, lang);
    dto.dimensions = TasklistStateDTO.dimensionStatus(dataset, revision, lang);
    dto.metadata = TasklistStateDTO.metadataStatus(revision, lang);
    dto.publishing = TasklistStateDTO.publishingStatus(dataset, revision);
    dto.translation = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

    const dimensionsComplete = isUpdate || every(dto.dimensions, (dim) => dim.status === TaskListStatus.Completed);
    const metadataComplete = isUpdate || every(dto.metadata, (status) => status === TaskListStatus.Completed);
    const publishingComplete = every(dto.publishing, (status) => status === TaskListStatus.Completed);
    const translationsComplete = [TaskListStatus.Completed, TaskListStatus.Unchanged].includes(dto.translation.import);

    dto.canPublish = dimensionsComplete && metadataComplete && translationsComplete && publishingComplete;

    logger.debug(
      `\nTasklistState: ${JSON.stringify(
        {
          dimensionsComplete,
          metadataComplete,
          translationsComplete,
          publishingComplete,
          canPublish: dto.canPublish
        },
        null,
        2
      )}`
    );

    return dto;
  }
}
