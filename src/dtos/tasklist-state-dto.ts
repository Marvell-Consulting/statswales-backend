import { every } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';
import { translatableMetadataKeys } from '../types/translatable-metadata';

import { DimensionStatus } from './dimension-status';

export class TasklistStateDTO {
    datatable: TaskStatus;
    measure?: DimensionStatus;
    dimensions: DimensionStatus[];

    metadata: {
        title: TaskStatus;
        summary: TaskStatus;
        statistical_quality: TaskStatus;
        data_sources: TaskStatus;
        related_reports: TaskStatus;
        update_frequency: TaskStatus;
        designation: TaskStatus;
        data_collection: TaskStatus;
        relevant_topics: TaskStatus;
    };

    translation: {
        export: TaskStatus;
        import: TaskStatus;
    };

    publishing: {
        organisation: TaskStatus;
        when: TaskStatus;
    };

    canPublish: boolean;

    public static translationStatus(dataset: Dataset): TaskStatus {
        const metaFullyTranslated = dataset.metadata?.every((info) => {
            return every(translatableMetadataKeys, (key) => {
                // ignore roundingDescription if rounding isn't applied, otherwise check some data exists
                return key === 'roundingDescription' && !info.roundingApplied ? true : Boolean(info[key]);
            });
        });

        return metaFullyTranslated ? TaskStatus.Completed : TaskStatus.Incomplete;
    }

    public static fromDataset(dataset: Dataset, lang: string): TasklistStateDTO {
        const info = dataset.metadata?.find((info) => info.language === lang);

        const measure = () => {
            if (!dataset.measure) {
                return undefined;
            }
            if (dataset.measure.joinColumn) {
                return {
                    name: dataset.measure.factTableColumn,
                    status: TaskStatus.Completed,
                    type: 'measure'
                };
            }
            return { name: dataset.measure.factTableColumn, status: TaskStatus.NotStarted, type: 'measure' };
        };

        const latestRevision = dataset.revisions[dataset.revisions.length - 1];

        const dimensions = dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
            if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

            const dimInfo: DimensionMetadata | undefined = dimension.metadata.find((i) => lang.includes(i.language));

            dimensionStatus.push({
                name: dimInfo?.name || 'unknown',
                status: dimension.extractor === null ? TaskStatus.NotStarted : TaskStatus.Completed,
                type: dimension.type
            });

            return dimensionStatus;
        }, []);

        const dto = new TasklistStateDTO();
        dto.datatable = dataset.revisions.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted;
        dto.measure = measure();
        dto.dimensions = dimensions;

        dto.metadata = {
            title: info?.title ? TaskStatus.Completed : TaskStatus.NotStarted,
            summary: info?.description ? TaskStatus.Completed : TaskStatus.NotStarted,
            statistical_quality: info?.quality ? TaskStatus.Completed : TaskStatus.NotStarted,
            data_collection: info?.collection ? TaskStatus.Completed : TaskStatus.NotStarted,
            data_sources: dataset.datasetProviders?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
            related_reports:
                info?.relatedLinks && info.relatedLinks?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted,
            update_frequency: info?.updateFrequency ? TaskStatus.Completed : TaskStatus.NotStarted,
            designation: info?.designation ? TaskStatus.Completed : TaskStatus.NotStarted,
            relevant_topics: dataset.datasetTopics?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted
        };

        const dimensionsComplete = every(dimensions, (dim) => dim.status === TaskStatus.Completed);
        const metadataComplete = every(dto.metadata, (status) => status === TaskStatus.Completed);

        const translationStatus = TasklistStateDTO.translationStatus(dataset);
        const translationsComplete = translationStatus === TaskStatus.Completed;

        // TODO: import should check export complete and nothing was updated since the export (needs audit table)
        dto.translation = {
            export: dimensionsComplete && metadataComplete ? TaskStatus.Available : TaskStatus.CannotStart,
            import: dimensionsComplete && metadataComplete ? translationStatus : TaskStatus.CannotStart
        };

        dto.publishing = {
            organisation: dataset.team ? TaskStatus.Completed : TaskStatus.NotStarted,
            when: latestRevision.publishAt ? TaskStatus.Completed : TaskStatus.NotStarted
        };

        const publishingComplete = every(dto.publishing, (status) => status === TaskStatus.Completed);

        dto.canPublish = dimensionsComplete && metadataComplete && translationsComplete && publishingComplete;

        return dto;
    }
}
