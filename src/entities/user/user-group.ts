import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  JoinTable,
  ManyToMany,
  ManyToOne,
  OneToMany,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from 'typeorm';

import { Dataset } from '../dataset/dataset';

import { Organisation } from './organisation';
import { User } from './user';
import { UserGroupMetadata } from './user-group-metadata';
import { UserGroupStatus } from '../../enums/user-group-status';

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

  @ManyToMany(() => User, (user) => user.groups)
  @JoinTable({
    name: 'user_group_user',
    joinColumn: {
      name: 'user_group_id',
      referencedColumnName: 'id',
      foreignKeyConstraintName: 'FK_user_group_user_user_group_id'
    },
    inverseJoinColumn: {
      name: 'user_id',
      referencedColumnName: 'id',
      foreignKeyConstraintName: 'FK_user_group_user_user_id'
    }
  })
  users?: User[];

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
