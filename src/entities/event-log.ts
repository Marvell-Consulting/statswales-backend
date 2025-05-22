import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn
} from 'typeorm';
import { User } from './user/user';

@Entity({ name: 'event_log' })
export class EventLog extends BaseEntity {
  @PrimaryGeneratedColumn({ name: 'id', primaryKeyConstraintName: 'PK_event_log_id' })
  id: string;

  @Column({ name: 'action', type: 'text', nullable: false })
  action: string; // the action that was performed, e.g. create, update, delete

  @Column({ name: 'entity', type: 'text', nullable: false })
  entity: string; // the entity that was affected, e.g. user, dataset, dimension, etc

  @Index('IDX_event_log_entity_id')
  @Column({ name: 'entity_id', type: 'text', nullable: false })
  entityId: string; // the id of the entity that was affected

  @Column({ name: 'data', type: 'jsonb', nullable: true })
  data?: Record<string, any>; // the new values of the record that was changed

  @Index('IDX_event_log_user_id')
  @Column({ name: 'user_id', type: 'uuid', nullable: true })
  userId?: string; // the user that triggered the event

  @ManyToOne(() => User, { nullable: true })
  @JoinColumn({ name: 'user_id', foreignKeyConstraintName: 'FK_event_log_user_id' })
  user?: User;

  @Column({ name: 'client', type: 'text', nullable: true })
  client?: string; // the client that initiated the event, e.g. sw3-frontend (or "system" if not an api call)

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date; // when the event happened
}
