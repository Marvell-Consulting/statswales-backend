import { every, first, sortBy } from 'lodash';

import { Dataset } from '../entities/dataset/dataset';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';
import { translatableMetadataKeys } from '../types/translatable-metadata';
import { DimensionStatus } from '../interfaces/dimension-status';
import { logger } from '../utils/logger';

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
            if (!dataset.measure) return undefined;

            return {
                id: dataset.measure.id,
                name: dataset.measure.factTableColumn,
                status: dataset.measure.joinColumn ? TaskStatus.Completed : TaskStatus.NotStarted,
                type: 'measure'
            };
        };

        const latestRevision = first(sortBy(dataset.revisions, 'created_at'));

        const dimensions = dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
            if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

            const dimInfo: DimensionMetadata | undefined = dimension.metadata.find((i) => lang.includes(i.language));
            const dimensionUpdateTask = latestRevision?.tasks?.dimensions.find((task) => task.id === dimension.id);
            if (dimensionUpdateTask && !dimensionUpdateTask.lookupTableUpdated) {
                dimensionStatus.push({
                    id: dimension.id,
                    name: dimInfo?.name || 'unknown',
                    status: TaskStatus.NotStarted,
                    type: dimension.type
                });
            } else {
                dimensionStatus.push({
                    id: dimension.id,
                    name: dimInfo?.name || 'unknown',
                    status: dimension.extractor === null ? TaskStatus.NotStarted : TaskStatus.Completed,
                    type: dimension.type
                });
            }

            return dimensionStatus;
        }, []);
        const dto = new TasklistStateDTO();
        dto.datatable = latestRevision?.dataTable ? TaskStatus.Completed : TaskStatus.NotStarted;
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
            when: latestRevision?.publishAt ? TaskStatus.Completed : TaskStatus.NotStarted
        };

        const publishingComplete = every(dto.publishing, (status) => status === TaskStatus.Completed);

        console.log('tasklist state: ', {
            dimensionsComplete,
            metadataComplete,
            translationsComplete,
            publishingComplete
        });

        dto.canPublish = dimensionsComplete && metadataComplete && translationsComplete && publishingComplete;

        return dto;
    }
}
