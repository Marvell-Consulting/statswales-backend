import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  JoinColumn,
  ManyToOne,
  PrimaryColumn,
  UpdateDateColumn
} from 'typeorm';

import { UserGroup } from './user-group';

@Entity({ name: 'user_group_metadata' })
export class UserGroupMetadata extends BaseEntity {
  @PrimaryColumn({
    name: 'user_group_id',
    type: 'uuid',
    primaryKeyConstraintName: 'PK_user_group_metadata_user_group_id_language'
  })
  id: string;

  @ManyToOne(() => UserGroup, (userGroup) => userGroup.metadata, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
  @JoinColumn({ name: 'user_group_id', foreignKeyConstraintName: 'FK_user_group_metadata_user_group_id' })
  userGroup: UserGroup;

  @PrimaryColumn({
    name: 'language',
    type: 'varchar',
    length: 5,
    primaryKeyConstraintName: 'PK_user_group_metadata_user_group_id_language'
  })
  language: string;

  @Column({ name: 'name', type: 'text', nullable: true })
  name?: string;

  @Column({ name: 'email', type: 'text', nullable: true })
  email?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
