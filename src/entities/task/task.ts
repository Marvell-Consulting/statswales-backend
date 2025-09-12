import {
  Entity,
  BaseEntity,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  Index,
  JoinColumn,
  ManyToOne,
  UpdateDateColumn,
  Column
} from 'typeorm';

import { User } from '../user/user';
import { TaskAction } from '../../enums/task-action';
import { TaskStatus } from '../../enums/task-status';
import { Dataset } from '../dataset/dataset';

export interface TaskMetadata {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

@Entity({ name: 'task' })
export class Task extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_task_id' })
  id: string;

  @Column({ name: 'action', type: 'text', nullable: false })
  action: TaskAction;

  @Index('IDX_task_status')
  @Column({ name: 'status', type: 'text', nullable: false })
  status: TaskStatus;

  @Index('IDX_task_open')
  @Column({ name: 'open', type: 'boolean', nullable: false, default: true })
  open: boolean;

  @Index('IDX_task_dataset_id')
  @Column({ name: 'dataset_id', type: 'text', nullable: true })
  datasetId?: string;

  @ManyToOne(() => Dataset, (dataset) => dataset.tasks, { nullable: true, onDelete: 'CASCADE' })
  @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_task_dataset_id' })
  dataset?: Dataset;

  @Column({ name: 'metadata', type: 'jsonb', nullable: true })
  metadata?: TaskMetadata;

  @Column({ name: 'comment', type: 'text', nullable: true })
  comment?: string | null;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  @Index('IDX_task_created_by')
  @ManyToOne(() => User, { nullable: true })
  @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_task_created_by' })
  createdBy: User | null;

  @Index('IDX_task_updated_by')
  @ManyToOne(() => User)
  @JoinColumn({ name: 'updated_by', foreignKeyConstraintName: 'FK_task_updated_by' })
  updatedBy: User | null;
}
