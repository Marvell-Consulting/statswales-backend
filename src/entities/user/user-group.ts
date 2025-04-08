import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from 'typeorm';

import { Dataset } from '../dataset/dataset';

import { Organisation } from './organisation';
import { UserGroupMetadata } from './user-group-metadata';
import { UserGroupStatus } from '../../enums/user-group-status';
import { UserGroupRole } from './user-group-role';

@Entity({ name: 'user_group' })
export class UserGroup extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_user_group_id' })
  id: string;

  @Column({ name: 'prefix', type: 'text', nullable: true })
  prefix?: string;

  @OneToMany(() => UserGroupMetadata, (meta) => meta.userGroup, { cascade: true })
  metadata: UserGroupMetadata[];

  @Column({ name: 'organisation_id', type: 'uuid', nullable: true })
  organisationId?: string;

  @Index('IDX_user_group_organisation_id')
  @ManyToOne(() => Organisation)
  @JoinColumn({ name: 'organisation_id', foreignKeyConstraintName: 'FK_user_group_organisation_id' })
  organisation?: Organisation;

  @OneToMany(() => Dataset, (dataset) => dataset.userGroup)
  datasets?: Dataset[];

  @OneToMany(() => UserGroupRole, (userGroupRole) => userGroupRole.group)
  groupRoles?: UserGroupRole[];

  @Column({
    name: 'status',
    type: 'enum',
    enum: Object.values(UserGroupStatus),
    nullable: false,
    default: UserGroupStatus.Active
  })
  status: UserGroupStatus;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
