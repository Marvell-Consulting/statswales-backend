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

import { User } from './user';
import { UserGroup } from './user-group';
import { GroupRole } from '../../enums/group-role';

@Entity({ name: 'user_group_role' })
export class UserGroupRole extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_user_group_role_id' })
  id: string;

  @Index('IDX_user_group_role_roles')
  @Column({ name: 'roles', type: 'jsonb', default: [] })
  roles: GroupRole[];

  @Index('IDX_user_group_role_user_id')
  @Column({ name: 'user_id', type: 'uuid' })
  userId: string;

  @ManyToOne(() => User, (user) => user.groupRoles, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'user_id', foreignKeyConstraintName: 'FK_user_group_role_user_id' })
  user: User;

  @Index('IDX_user_group_role_group_id')
  @Column({ name: 'group_id', type: 'uuid' })
  groupId: string;

  @ManyToOne(() => UserGroup, (userGroup) => userGroup.groupRoles, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'group_id', foreignKeyConstraintName: 'FK_user_group_role_group_id' })
  group: UserGroup;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;
}
