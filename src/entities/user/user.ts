import {
  BaseEntity,
  Column,
  CreateDateColumn,
  Entity,
  Index,
  OneToMany,
  PrimaryGeneratedColumn,
  UpdateDateColumn
} from 'typeorm';

import { UserStatus } from '../../enums/user-status';
import { GlobalRole } from '../../enums/global-role';

import { UserGroupRole } from './user-group-role';
import { Dataset } from '../dataset/dataset';

@Entity({ name: 'user' })
@Index('IDX_user_provider_provider_user_id', ['provider', 'providerUserId'])
export class User extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_user_id' })
  id: string;

  @Index('IDX_user_provider')
  @Column({ name: 'provider', type: 'text' })
  provider: string;

  @Column({ name: 'provider_user_id', type: 'text', nullable: true })
  providerUserId?: string;

  @Index('UX_user_email', { unique: true })
  @Column({ name: 'email', type: 'text', nullable: false })
  email: string;

  @Column({ name: 'name', type: 'text', nullable: true })
  name?: string;

  @OneToMany(() => UserGroupRole, (userGroupRole) => userGroupRole.user, { cascade: true })
  groupRoles: UserGroupRole[];

  @Column({ name: 'global_roles', type: 'jsonb', default: [] })
  globalRoles: GlobalRole[];

  @Index('IDX_user_status')
  @Column({
    name: 'status',
    type: 'enum',
    enum: Object.values(UserStatus),
    default: UserStatus.Active,
    nullable: false
  })
  status: UserStatus;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  @Column({ name: 'last_login_at', type: 'timestamptz', nullable: true })
  lastLoginAt: Date;

  @OneToMany(() => Dataset, (dataset) => dataset.createdBy)
  datasets: Dataset[];
}
