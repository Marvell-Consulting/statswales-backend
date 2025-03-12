import { BaseEntity, Column, Entity, Index, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';

import { Topic } from './topic';
import { Revision } from './revision';

@Entity({ name: 'revision_topic' })
export class RevisionTopic extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_revision_topic_id' })
  id: string;

  @Index('IDX_revision_topic_revision_id')
  @Column({ type: 'uuid', name: 'revision_id' })
  revisionId: string;

  @ManyToOne(() => Revision, (revision) => revision.revisionTopics, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete'
  })
  @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_revision_topic_revision_id' })
  revision: Revision;

  @Index('IDX_revision_topic_topic_id')
  @Column({ type: 'int', name: 'topic_id' })
  topicId: number;

  @ManyToOne(() => Topic, (topic) => topic.revisionTopics, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
  @JoinColumn({ name: 'topic_id', foreignKeyConstraintName: 'FK_revision_topic_topic_id' })
  topic: Topic;
}
