import { BaseEntity, Column, Entity, Index, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';

import { Dataset } from './dataset';
import { Topic } from './topic';

@Entity({ name: 'dataset_topic' })
export class DatasetTopic extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_dataset_topic_id' })
    id: string;

    @Index('IDX_dataset_topic_dataset_id')
    @Column({ type: 'uuid', name: 'dataset_id' })
    datasetId: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetTopics, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_topic_dataset_id' })
    dataset: Dataset;

    @Index('IDX_dataset_topic_topic_id')
    @Column({ type: 'int', name: 'topic_id' })
    topicId: number;

    @ManyToOne(() => Topic, (topic) => topic.datasetTopics, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'topic_id', foreignKeyConstraintName: 'FK_dataset_topic_topic_id' })
    topic: Topic;
}
