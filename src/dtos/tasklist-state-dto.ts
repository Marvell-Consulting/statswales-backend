import { every } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { DimensionStatus } from '../interfaces/dimension-status';
import { logger } from '../utils/logger';
import { Revision } from '../entities/dataset/revision';

export interface Metadata {
    title: TaskStatus;
    summary: TaskStatus;
    statistical_quality: TaskStatus;
    update_frequency: TaskStatus;
    designation: TaskStatus;
    data_collection: TaskStatus;
    related_reports: TaskStatus;
    data_sources: TaskStatus;
    relevant_topics: TaskStatus;
}

export class TasklistStateDTO {
    datatable: TaskStatus;
    measure?: DimensionStatus;
    dimensions: DimensionStatus[];

    metadata: Metadata;

    translation: {
        export: TaskStatus;
        import: TaskStatus;
    };

    publishing: {
        organisation: TaskStatus;
        when: TaskStatus;
    };

    canPublish: boolean;

    public static measureStatus(dataset: Dataset) {
        if (!dataset.measure) return undefined;

        return {
            id: dataset.measure.id,
            name: dataset.measure.factTableColumn,
            status: dataset.measure.joinColumn ? TaskStatus.Completed : TaskStatus.NotStarted,
            type: 'measure'
        };
    }

    public static dimensionStatus(dataset: Dataset, revision: Revision, lang: string): DimensionStatus[] {
        return dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
            if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

            const name = dimension.metadata.find((meta) => lang.includes(meta.language))?.name ?? 'unknown';

            const isUpdate = revision.tasks?.dimensions.find((task) => task.id === dimension.id);

            const status =
                (isUpdate && !isUpdate.lookupTableUpdated) || dimension.extractor === null
                    ? TaskStatus.NotStarted
                    : TaskStatus.Completed;

            dimensionStatus.push({ name, status, id: dimension.id, type: dimension.type });

            return dimensionStatus;
        }, []);
    }

    public static metadataStatus(revision: Revision, lang: string): Metadata {
        const metadata = revision?.metadata.find((meta) => lang.includes(meta.language));

        return {
            title: metadata?.title ? TaskStatus.Completed : TaskStatus.NotStarted,
            summary: metadata?.summary ? TaskStatus.Completed : TaskStatus.NotStarted,
            statistical_quality: metadata?.quality ? TaskStatus.Completed : TaskStatus.NotStarted,
            data_collection: metadata?.collection ? TaskStatus.Completed : TaskStatus.NotStarted,
            update_frequency: revision.updateFrequency ? TaskStatus.Completed : TaskStatus.NotStarted,
            designation: revision?.designation ? TaskStatus.Completed : TaskStatus.NotStarted,
            data_sources: revision?.revisionProviders?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
            relevant_topics: revision?.revisionTopics?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
            related_reports:
                revision?.relatedLinks && revision.relatedLinks?.length > 0
                    ? TaskStatus.Completed
                    : TaskStatus.NotStarted
        };
    }

    public static publishingStatus(dataset: Dataset, revision: Revision) {
        return {
            organisation: dataset.team ? TaskStatus.Completed : TaskStatus.NotStarted,
            when: revision.publishAt ? TaskStatus.Completed : TaskStatus.NotStarted
        };
    }

    public static translationStatus(revision: Revision): TaskStatus {
        const metaFullyTranslated = revision.metadata?.every((meta) => {
            return every(translatableMetadataKeys, (key) => {
                // ignore roundingDescription if rounding isn't applied, otherwise check some data exists
                return key === 'roundingDescription' && !revision.roundingApplied ? true : Boolean(meta[key]);
            });
        });

        return metaFullyTranslated ? TaskStatus.Completed : TaskStatus.Incomplete;
    }

    public static fromDataset(dataset: Dataset, revision: Revision, lang: string): TasklistStateDTO {
        const dto = new TasklistStateDTO();
        dto.datatable = revision?.dataTable ? TaskStatus.Completed : TaskStatus.NotStarted;
        dto.measure = TasklistStateDTO.measureStatus(dataset);
        dto.dimensions = TasklistStateDTO.dimensionStatus(dataset, revision, lang);
        dto.metadata = TasklistStateDTO.metadataStatus(revision, lang);
        dto.publishing = TasklistStateDTO.publishingStatus(dataset, revision);

        const dimensionsComplete = every(dto.dimensions, (dim) => dim.status === TaskStatus.Completed);
        const metadataComplete = every(dto.metadata, (status) => status === TaskStatus.Completed);
        const publishingComplete = every(dto.publishing, (status) => status === TaskStatus.Completed);

        const translationStatus = TasklistStateDTO.translationStatus(revision);
        const translationsComplete = translationStatus === TaskStatus.Completed;

        // TODO: import should check export complete and nothing was updated since the export (needs audit table)
        dto.translation = {
            export: dimensionsComplete && metadataComplete ? TaskStatus.Available : TaskStatus.CannotStart,
            import: dimensionsComplete && metadataComplete ? translationStatus : TaskStatus.CannotStart
        };

        dto.canPublish = dimensionsComplete && metadataComplete && translationsComplete && publishingComplete;

        return dto;
    }
}
