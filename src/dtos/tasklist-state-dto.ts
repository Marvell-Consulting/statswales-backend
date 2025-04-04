import { every, isEqual, max, pick, sortBy } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { DimensionStatus } from '../interfaces/dimension-status';
import { Revision } from '../entities/dataset/revision';
import { EventLog } from '../entities/event-log';
import { logger } from '../utils/logger';
import { TranslationDTO } from './translations-dto';
import { collectTranslations } from '../utils/collect-translations';

export interface MetadataStatus {
  title: TaskStatus;
  summary: TaskStatus;
  quality: TaskStatus;
  collection: TaskStatus;
  frequency: TaskStatus;
  designation: TaskStatus;
  related: TaskStatus;
  sources: TaskStatus;
  topics: TaskStatus;
}

export interface TranslationStatus {
  export: TaskStatus;
  import: TaskStatus;
}

export interface PublishingStatus {
  when: TaskStatus;
}

export class TasklistStateDTO {
  datatable: TaskStatus;
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
      return uploadedAt && uploadedAt > revision.createdAt ? TaskStatus.Updated : TaskStatus.Unchanged;
    }

    return revision?.dataTable ? TaskStatus.Completed : TaskStatus.NotStarted;
  }

  public static measureStatus(dataset: Dataset, revision: Revision, lang: string) {
    if (!dataset.measure) return undefined;

    const measure = dataset.measure;
    const isUpdate = Boolean(revision.previousRevisionId);
    let status = TaskStatus.NotStarted;

    if (isUpdate) {
      status = TaskStatus.Unchanged;
    } else if (measure.joinColumn) {
      status = TaskStatus.Completed;
    }

    const name = measure.metadata?.find((meta) => meta.language.includes(lang))?.name ?? measure.factTableColumn;

    return { type: 'measure', id: measure.id, name, status };
  }

  public static dimensionStatus(dataset: Dataset, revision: Revision, lang: string): DimensionStatus[] {
    const isUpdate = Boolean(revision.previousRevisionId);

    return dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
      if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

      const name = dimension.metadata.find((meta) => lang.includes(meta.language))?.name ?? 'unknown';
      let status: TaskStatus;

      if (isUpdate) {
        status = TaskStatus.Unchanged;
        const updateTask = revision.tasks?.dimensions.find((task) => task.id === dimension.id);

        if (updateTask) {
          status = updateTask.lookupTableUpdated ? TaskStatus.Updated : TaskStatus.Unchanged;
        }
      } else {
        status = dimension.extractor === null ? TaskStatus.NotStarted : TaskStatus.Completed;
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
        title: isEqual(prevMeta.title, metadata.title) ? TaskStatus.Unchanged : TaskStatus.Updated,
        summary: isEqual(prevMeta.summary, metadata.summary) ? TaskStatus.Unchanged : TaskStatus.Updated,
        quality: isEqual(prevMeta.quality, metadata.quality) ? TaskStatus.Unchanged : TaskStatus.Updated,
        collection: isEqual(prevMeta.collection, metadata.collection) ? TaskStatus.Unchanged : TaskStatus.Updated,
        frequency: isEqual(prevRevision.updateFrequency, revision.updateFrequency)
          ? TaskStatus.Unchanged
          : TaskStatus.Updated,
        designation: prevRevision.designation === revision.designation ? TaskStatus.Unchanged : TaskStatus.Updated,
        sources: isEqual(providers, prevProviders) ? TaskStatus.Unchanged : TaskStatus.Updated,
        topics: isEqual(topics, prevTopics) ? TaskStatus.Unchanged : TaskStatus.Updated,
        related: isEqual(prevRevision.relatedLinks, revision.relatedLinks) ? TaskStatus.Unchanged : TaskStatus.Updated
      };
    }

    return {
      title: metadata?.title ? TaskStatus.Completed : TaskStatus.NotStarted,
      summary: metadata?.summary ? TaskStatus.Completed : TaskStatus.NotStarted,
      quality: metadata?.quality ? TaskStatus.Completed : TaskStatus.NotStarted,
      collection: metadata?.collection ? TaskStatus.Completed : TaskStatus.NotStarted,
      frequency: revision.updateFrequency ? TaskStatus.Completed : TaskStatus.NotStarted,
      designation: revision?.designation ? TaskStatus.Completed : TaskStatus.NotStarted,
      sources: revision?.revisionProviders?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
      topics: revision?.revisionTopics?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
      related:
        revision?.relatedLinks && revision.relatedLinks?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted
    };
  }

  public static publishingStatus(dataset: Dataset, revision: Revision): PublishingStatus {
    return {
      when: revision.publishAt ? TaskStatus.Completed : TaskStatus.NotStarted
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
          import: TaskStatus.Unchanged,
          export: TaskStatus.Unchanged
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

    const lastExport = translationEvents?.find((event) => event.action === 'export');

    const existingTranslations = collectTranslations(dataset);

    // previously exported revisions did not include data, ignore these.
    const exportStale = lastExport?.data?.translations.some((incoming: TranslationDTO) => {
      const expected = existingTranslations.find((existing) => existing.key === incoming.key)?.english;
      // previous export value has since been updated
      return expected !== incoming.english;
    });

    const translationRequired = !metadataSynced || !metaFullyTranslated;
    const exportStatus = lastExportedAt
      ? exportStale
        ? TaskStatus.Incomplete
        : TaskStatus.Completed
      : TaskStatus.NotStarted;
    const importStatus =
      lastImportedAt && lastImportedAt > lastMetaUpdateAt ? TaskStatus.Completed : TaskStatus.NotStarted;

    return {
      export: translationRequired ? exportStatus : TaskStatus.NotRequired,
      import: translationRequired ? importStatus : TaskStatus.NotRequired
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

    const dimensionsComplete = isUpdate || every(dto.dimensions, (dim) => dim.status === TaskStatus.Completed);
    const metadataComplete = isUpdate || every(dto.metadata, (status) => status === TaskStatus.Completed);
    const publishingComplete = every(dto.publishing, (status) => status === TaskStatus.Completed);
    const translationsComplete = dto.translation.import === TaskStatus.Completed;

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
