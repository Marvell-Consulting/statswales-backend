import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    JoinColumn, OneToOne
} from 'typeorm';

import { Filetype } from '../../enums/filetype';
import { Dimension } from './dimension';
import { Measure } from './measure';

@Entity({ name: 'lookup_table', orderBy: { uploadedAt: 'ASC' } })
export class LookupTable extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_lookup_table_id' })
    id: string;

    @OneToOne(() => Dimension, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dimension_id', foreignKeyConstraintName: 'FK_lookup_dimension_id' })
    dimension: Dimension;

    @OneToOne(() => Measure, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'measure_id', foreignKeyConstraintName: 'FK_lookup_measure_id' })
    measure: Measure;

    @Column({ name: 'mime_type', type: 'varchar', length: 255 })
    mimeType: string;

    @Column({ name: 'filetype', type: 'enum', enum: Object.values(Filetype), nullable: false })
    fileType: Filetype;

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
}
