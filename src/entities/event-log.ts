import { BaseEntity, Column, CreateDateColumn, Entity, PrimaryGeneratedColumn } from 'typeorm';

@Entity({ name: 'event_log' })
export class EventLog extends BaseEntity {
    @PrimaryGeneratedColumn({ name: 'id', primaryKeyConstraintName: 'PK_event_log_id' })
    id: string;

    @Column({ name: 'action', type: 'text', nullable: false })
    action: string; // the action that was performed, e.g. create, update, delete

    @Column({ name: 'entity', type: 'text', nullable: false })
    entity: string; // the entity that was affected, e.g. user, dataset, dimension, etc

    @Column({ name: 'entity_id', type: 'text', nullable: false })
    entityId: string; // the id of the entity that was affected

    @Column({ name: 'data', type: 'jsonb', nullable: true })
    data: Record<string, any>; // the new values of the record that was changed

    @Column({ name: 'user_id', type: 'uuid', nullable: true })
    userId: string; // the user that triggered the event

    @Column({ name: 'client', type: 'text', nullable: true })
    client: string; // the client that initiated the event, e.g. sw3-frontend (or "system" if not an api call)

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date; // when the event happened
}
