import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  BaseEntity,
  ManyToOne,
  OneToMany,
  JoinColumn,
  OneToOne,
  Index
} from 'typeorm';

import { User } from '../user/user';

import { Revision } from './revision';
import { Dimension } from './dimension';
import { Measure } from './measure';
import { FactTableColumn } from './fact-table-column';
import { UserGroup } from '../user/user-group';

@Entity({ name: 'dataset' })
export class Dataset extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_dataset_id' })
  id: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Index('IDX_dataset_created_by')
  @ManyToOne(() => User)
  @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_dataset_created_by' })
  createdBy: User;

  @Column({ type: 'uuid', name: 'created_by' })
  createdById: string;

  @Column({ type: 'timestamptz', nullable: true })
  live: Date | null;

  @Column({ type: 'timestamptz', nullable: true })
  archive: Date;

  @Column({ name: 'start_date', type: 'date', nullable: true })
  startDate: Date | null;

  @Column({ name: 'end_date', type: 'date', nullable: true })
  endDate: Date | null;

  @OneToMany(() => Dimension, (dimension) => dimension.dataset, { cascade: true })
  dimensions: Dimension[];

  @OneToMany(() => Revision, (revision) => revision.dataset, { cascade: true })
  revisions: Revision[];

  @Column({ name: 'start_revision_id', type: 'uuid', nullable: true })
  startRevisionId?: string;

  // the very first revision
  @OneToOne(() => Revision, (revision) => revision.dataset, { nullable: true })
  @JoinColumn({ name: 'start_revision_id', foreignKeyConstraintName: 'FK_dataset_start_revision_id' })
  startRevision: Revision | null;

  @Column({ name: 'end_revision_id', type: 'uuid', nullable: true })
  endRevisionId?: string;

  // the newest revision (including draft revision if in progress)
  @OneToOne(() => Revision, (revision) => revision.dataset, { nullable: true })
  @JoinColumn({ name: 'end_revision_id', foreignKeyConstraintName: 'FK_dataset_end_revision_id' })
  endRevision: Revision | null;

  @Column({ name: 'draft_revision_id', type: 'uuid', nullable: true })
  draftRevisionId?: string;

  // the currently in progress unpublished (initial or update) revision or NULL if none in progress
  @OneToOne(() => Revision, (revision) => revision.dataset, { nullable: true, cascade: true, onDelete: 'SET NULL' })
  @JoinColumn({ name: 'draft_revision_id', foreignKeyConstraintName: 'FK_dataset_draft_revision_id' })
  draftRevision: Revision | null;

  // the most recent published aka "live" revision or NULL if unpublished
  @OneToOne(() => Revision, (revision) => revision.dataset, { nullable: true })
  @JoinColumn({ name: 'published_revision_id', foreignKeyConstraintName: 'FK_dataset_published_revision_id' })
  publishedRevision: Revision | null;

  @OneToOne(() => Measure, (measure) => measure.dataset, { cascade: true })
  measure: Measure;

  @OneToMany(() => FactTableColumn, (factTableColumn) => factTableColumn.dataset, { cascade: true })
  factTable: FactTableColumn[] | null;

  @Column({ name: 'user_group_id', type: 'uuid', nullable: true })
  userGroupId?: string;

  @Index('IDX_dataset_user_group_id')
  @ManyToOne(() => UserGroup, (userGroup) => userGroup.datasets, { nullable: true })
  @JoinColumn({ name: 'user_group_id', foreignKeyConstraintName: 'FK_dataset_user_group_id' })
  userGroup: UserGroup;
}
