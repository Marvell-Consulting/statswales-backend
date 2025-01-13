import { DatasetTopic } from '../entities/dataset/dataset-topic';

import { TopicDTO } from './topic-dto';

export class DatasetTopicDTO {
    id: string;
    dataset_id: string;
    topic_id: number;
    topic: TopicDTO;

    static fromDatasetTopic(datasetTopic: DatasetTopic): DatasetTopicDTO {
        const dto = new DatasetTopicDTO();
        dto.id = datasetTopic.id;
        dto.dataset_id = datasetTopic.datasetId;
        dto.topic_id = datasetTopic.topicId;
        dto.topic = TopicDTO.fromTopic(datasetTopic.topic);

        return dto;
    }
}
