import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, JoinColumn, OneToOne } from 'typeorm';

import { FileType } from '../../enums/file-type';

import { Dimension } from './dimension';
import { Measure } from './measure';
import { FileImport } from './file-import';

@Entity({ name: 'lookup_table', orderBy: { uploadedAt: 'ASC' } })
export class LookupTable extends BaseEntity implements FileImport {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_lookup_table_id' })
    id: string;

    @OneToOne(() => Dimension, {
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dimension_id' })
    dimension: Dimension;

    @OneToOne(() => Measure, {
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'measure_id' })
    measure: Measure;

    @Column({ name: 'mime_type', type: 'varchar', length: 255 })
    mimeType: string;

    @Column({ name: 'filetype', type: 'enum', enum: Object.values(FileType), nullable: false })
    fileType: FileType;

    @Column({ type: 'varchar', length: 255 })
    filename: string;

    @Column({ type: 'varchar', length: 255 })
    hash: string;

    @CreateDateColumn({ name: 'uploaded_at', type: 'timestamptz' })
    uploadedAt: Date;

    @Column({ name: 'delimiter', type: 'char' })
    delimiter: string;

    @Column({ name: 'quote', type: 'char' })
    quote: string;

    @Column({ name: 'linebreak', type: 'varchar' })
    linebreak: string;

    @Column({ name: 'is_statswales2_format', type: 'boolean' })
    isStatsWales2Format: boolean;
}
