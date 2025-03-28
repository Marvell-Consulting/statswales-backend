import { BaseEntity, CreateDateColumn, Entity, OneToMany, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

import { OrganisationMetadata } from './organisation-metadata';

@Entity({ name: 'organisation' })
export class Organisation extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_organisation_id' })
  id: string;

  @OneToMany(() => OrganisationMetadata, (meta) => meta.organisation, { cascade: true })
  metadata: OrganisationMetadata[];

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
