import { Dataset } from '../entities/dataset/dataset';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';

import { DimensionStatus } from './dimension-status';
import { MeasureDTO } from './measure-dto';

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

    public static fromDataset(dataset: Dataset, lang: string): TasklistStateDTO {
        const info = dataset.datasetInfo?.find((info) => info.language === lang);
      
        const measure = () => {
            if (!dataset.measure) {
                return undefined;
            }
            if (dataset.measure.joinColumn) {
                return {
                    name: dataset.measure.factTableColumn,
                    status: TaskStatus.Completed
                };
            }
            return { name: dataset.measure.factTableColumn, status: TaskStatus.NotStarted };
        };

        const latestRevision = dataset.revisions[dataset.revisions.length - 1];

        const dimensions = dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
            if (dimension.type === DimensionType.NoteCodes) return dimensionStatus;

            const dimInfo: DimensionInfo | undefined = dimension.dimensionInfo.find((i) => lang.includes(i.language));

            dimensionStatus.push({
                name: dimInfo?.name || 'unknown',
                status: dimension.type === DimensionType.Raw ? TaskStatus.NotStarted : TaskStatus.Completed
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
            related_reports: info?.relatedLinks ? TaskStatus.Completed : TaskStatus.NotStarted,
            update_frequency: info?.updateFrequency ? TaskStatus.Completed : TaskStatus.NotStarted,
            designation: info?.designation ? TaskStatus.Completed : TaskStatus.NotStarted,
            relevant_topics: dataset.datasetTopics?.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted
        };

        dto.translation = {
            export: TaskStatus.NotImplemented,
            import: TaskStatus.NotImplemented
        };

        dto.publishing = {
            organisation: TaskStatus.NotImplemented,
            when: latestRevision.publishAt ? TaskStatus.Completed : TaskStatus.NotStarted
        };

        return dto;
    }
}
