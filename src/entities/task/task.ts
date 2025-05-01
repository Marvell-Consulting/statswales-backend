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

  @Index('IDX_task_entity_entity_id', ['entity', 'entity_id'])
  @Column({ name: 'entity', type: 'text', nullable: true })
  entity?: string;

  @Column({ name: 'entity_id', type: 'text', nullable: true })
  entityId?: string;

  @Column({ name: 'comment', type: 'text', nullable: true })
  comment?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  @Index('IDX_task_submitted_by')
  @ManyToOne(() => User, { nullable: true })
  @JoinColumn({ name: 'submitted_by', foreignKeyConstraintName: 'FK_task_submitted_by' })
  submittedBy: User | null;

  @Index('IDX_task_response_by')
  @ManyToOne(() => User)
  @JoinColumn({ name: 'response_by', foreignKeyConstraintName: 'FK_task_response_by' })
  responseBy: User | null;
}
