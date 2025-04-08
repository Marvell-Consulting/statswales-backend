import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, OneToOne } from 'typeorm';

import { FileType } from '../../enums/file-type';

import { Dimension } from './dimension';
import { Measure } from './measure';
import { FileImportInterface } from './file-import.interface';

@Entity({ name: 'lookup_table', orderBy: { uploadedAt: 'ASC' } })
export class LookupTable extends BaseEntity implements FileImportInterface {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_lookup_table_id' })
  id: string;

  @OneToOne(() => Dimension, { orphanedRowAction: 'delete' })
  dimension: Dimension;

  @OneToOne(() => Measure, { orphanedRowAction: 'delete' })
  measure: Measure;

  @Column({ name: 'mime_type', type: 'varchar', length: 255 })
  mimeType: string;

  @Column({ name: 'filetype', type: 'enum', enum: Object.values(FileType), nullable: false })
  fileType: FileType;

  @Column({ type: 'varchar', length: 255 })
  filename: string;

  @Column({ name: 'original_filename', type: 'varchar', length: 255, nullable: true })
  originalFilename: string | null;

  @Column({ type: 'varchar', length: 255 })
  hash: string;

  @CreateDateColumn({ name: 'uploaded_at', type: 'timestamptz' })
  uploadedAt: Date;

  @Column({ name: 'is_statswales2_format', type: 'boolean' })
  isStatsWales2Format: boolean;
}
