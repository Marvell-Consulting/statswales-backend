import { Dataset } from '../entities/dataset/dataset';
import { DimensionInfo } from '../entities/dataset/dimension-info';
import { DimensionType } from '../enums/dimension-type';
import { TaskStatus } from '../enums/task-status';

import { DimensionStatus } from './dimension-status';

export class TasklistStateDTO {
    datatable: TaskStatus;

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

    publishing: {
        when: TaskStatus;
        export: TaskStatus;
        import: TaskStatus;
        submit: TaskStatus;
    };

    public static fromDataset(dataset: Dataset, lang: string): TasklistStateDTO {
        const info = dataset.datasetInfo?.find((info) => info.language === lang);

        const dimensions = dataset.dimensions?.reduce((dimensionStatus: DimensionStatus[], dimension) => {
            if (dimension.type === DimensionType.FootNote) return dimensionStatus;

            const info: DimensionInfo | undefined = dimension.dimensionInfo.find((i) => lang.includes(i.language));
            const csvColName = dimension.sources?.find((source) => source.dimension.id === dimension.id)?.csvField;

            dimensionStatus.push({
                name: info?.name || csvColName || 'unknown',
                status: dimension.type === DimensionType.Raw ? TaskStatus.NotStarted : TaskStatus.Completed
            });

            return dimensionStatus;
        }, []);

        const dto = new TasklistStateDTO();
        dto.datatable = dataset.revisions.length > 0 ? TaskStatus.Completed : TaskStatus.NotStarted;

        dto.dimensions = dimensions;

        dto.metadata = {
            title: info?.title ? TaskStatus.Completed : TaskStatus.NotStarted,
            summary: TaskStatus.NotImplemented,
            statistical_quality: TaskStatus.NotImplemented,
            data_sources: TaskStatus.NotImplemented,
            related_reports: TaskStatus.NotImplemented,
            update_frequency: TaskStatus.NotImplemented,
            designation: TaskStatus.NotImplemented,
            data_collection: TaskStatus.NotImplemented,
            relevant_topics: TaskStatus.NotImplemented
        };

        dto.publishing = {
            when: TaskStatus.NotImplemented,
            export: TaskStatus.NotImplemented,
            import: TaskStatus.NotImplemented,
            submit: TaskStatus.NotImplemented
        };

        return dto;
    }
}