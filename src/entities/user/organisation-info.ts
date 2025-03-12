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

import { Organisation } from './organisation';

@Entity({ name: 'organisation_info' })
export class OrganisationInfo extends BaseEntity {
  @PrimaryColumn({
    name: 'organisation_id',
    type: 'uuid',
    primaryKeyConstraintName: 'PK_organisation_info_organisation_id_language'
  })
  id: string;

  @ManyToOne(() => Organisation, (org) => org.info, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
  @JoinColumn({ name: 'organisation_id', foreignKeyConstraintName: 'FK_organisation_info_organisation_id' })
  organisation: Organisation;

  @PrimaryColumn({
    name: 'language',
    type: 'varchar',
    length: 5,
    primaryKeyConstraintName: 'PK_organisation_info_organisation_id_language'
  })
  language: string;

  @Column({ name: 'name', type: 'text', nullable: false })
  name: string;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
