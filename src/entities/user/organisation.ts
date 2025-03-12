import { BaseEntity, CreateDateColumn, Entity, OneToMany, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

import { OrganisationInfo } from './organisation-info';

@Entity({ name: 'organisation' })
export class Organisation extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_organisation_id' })
  id: string;

  @OneToMany(() => OrganisationInfo, (info) => info.organisation, { cascade: true })
  info: OrganisationInfo[];

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
