import {
  Entity,
  PrimaryColumn,
  Column,
  BaseEntity,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn
} from 'typeorm';

import { MetadataInterface } from '../../interfaces/metadata-interface';

import { Dimension } from './dimension';

@Entity({ name: 'dimension_metadata' })
export class DimensionMetadata extends BaseEntity implements MetadataInterface {
  @PrimaryColumn({
    name: 'dimension_id',
    type: 'uuid',
    primaryKeyConstraintName: 'PK_dimension_metadata_dimension_id_language'
  })
  id: string;

  @PrimaryColumn({
    name: 'language',
    type: 'varchar',
    length: 5,
    primaryKeyConstraintName: 'PK_dimension_metadata_dimension_id_language'
  })
  language: string;

  @Column({ type: 'text' })
  name: string;

  @Column({ type: 'text', nullable: true })
  description: string;

  @Column({ type: 'text', nullable: true })
  notes: string;

  @ManyToOne(() => Dimension, (dimension) => dimension.metadata, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete'
  })
  @JoinColumn({ name: 'dimension_id', foreignKeyConstraintName: 'FK_dimension_metadata_dimension_id' })
  dimension: Dimension;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  // This column should be the same across all languages
  // If one is updated and the others aren't then mark as needing translation
  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
  updatedAt: Date;
}
