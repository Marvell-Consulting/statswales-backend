import { BaseEntity, Column, CreateDateColumn, Entity, Index, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

@Entity({ name: 'user' })
@Index('UX_user_provider_provider_user_id', ['provider', 'providerUserId'], { unique: true })
export class User extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_user_id' })
  id: string;

  @Index('IX_user_provider')
  @Column({ name: 'provider' })
  provider: string;

  @Column({ name: 'provider_user_id' })
  providerUserId: string;

  @Index('UX_user_email', { unique: true })
  @Column({ name: 'email' })
  email: string;

  @Column({ name: 'email_verified', default: false })
  emailVerified: boolean;

  @Column({ name: 'given_name', nullable: true })
  givenName?: string;

  @Column({ name: 'family_name', nullable: true })
  familyName?: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;

  get name(): string {
    return `${this.givenName} ${this.familyName}`;
  }
}
