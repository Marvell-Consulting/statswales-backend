import { DatasetTopic } from '../entities/dataset/dataset-topic';

export class DatasetTopicDTO {
    id: string;
    dataset_id: string;
    topic_id: number;

    static fromDatasetTopic(datasetTopic: DatasetTopic): DatasetTopicDTO {
        const dto = new DatasetTopicDTO();
        dto.id = datasetTopic.id;
        dto.dataset_id = datasetTopic.datasetId;
        dto.topic_id = datasetTopic.topicId;
        return dto;
    }
}
