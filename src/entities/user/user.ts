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
import { UserGroupRole } from './user-group-role';
import { UserStatus } from '../../enums/user-status';

@Entity({ name: 'user' })
@Index('IDX_user_provider_provider_user_id', ['provider', 'providerUserId'])
export class User extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_user_id' })
  id: string;

  @Index('IDX_user_provider')
  @Column({ name: 'provider', type: 'text' })
  provider: string;

  @Column({ name: 'provider_user_id', nullable: true })
  providerUserId?: string;

  @Index('UX_user_email', { unique: true })
  @Column({ name: 'email', type: 'text', nullable: false })
  email: string;

  @Column({ name: 'email_verified', default: false })
  emailVerified: boolean;

  @Column({ name: 'given_name', type: 'text', nullable: true })
  givenName?: string;

  @Column({ name: 'family_name', type: 'text', nullable: true })
  familyName?: string;

  @OneToMany(() => UserGroupRole, (userGroupRole) => userGroupRole.user)
  groupRoles: UserGroupRole[];

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

  get name(): string | undefined {
    return `${this.givenName || ''} ${this.familyName || ''}`.trim() || undefined;
  }
}
