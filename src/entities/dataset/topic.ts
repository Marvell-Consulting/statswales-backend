import { BaseEntity, Column, Entity, ManyToMany, OneToMany, PrimaryGeneratedColumn } from 'typeorm';

import { DatasetTopic } from './dataset-topic';

@Entity({ name: 'topic' })
export class Topic extends BaseEntity {
    @PrimaryGeneratedColumn({ primaryKeyConstraintName: 'PK_topic_id' })
    id: number;

    // path stores the topic hierarchy, using dot notation
    // For root topics, the path is just the id, e.g. if id = 1, then the path is '1'
    // For child topics, the path contains the parent topic ids, e.g. for a topic with
    // grandparent id 1, parent id of 12 and an id of 57, then the path is '1.12.57'
    // This uses the ltree type in PostgreSQL, but we could just use text if we don't need the extra features
    // See https://www.postgresql.org/docs/current/ltree.html
    // and https://patshaughnessy.net/2017/12/13/saving-a-tree-in-postgres-using-ltree
    @Column({ name: 'path', type: 'ltree', nullable: false })
    path: string;

    @Column({ name: 'name_en', type: 'text', nullable: true })
    nameEN: string;

    @Column({ name: 'name_cy', type: 'text', nullable: true })
    nameCY: string;

    @OneToMany(() => DatasetTopic, (datasetTopic) => datasetTopic.topic, { cascade: true })
    datasetTopics: DatasetTopic[];
}