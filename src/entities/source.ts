import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { SourceAction } from '../enums/source-action';

// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';
// eslint-disable-next-line import/no-cycle
import { Import } from './import';
// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
import { SourceType } from './source_type';

@Entity()
export class Source extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_source_id' })
    id: string;

    @ManyToOne(() => Dimension, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dimension_id', foreignKeyConstraintName: 'FK_source_dimension_id' })
    dimension: Promise<Dimension>;

    @ManyToOne(() => Import, (importEntity) => importEntity.sources, {
        nullable: false,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id', foreignKeyConstraintName: 'FK_source_import_id' })
    import: Promise<Import>;

    @ManyToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_source_revision_id' })
    revision: Promise<Revision>;

    // Not implemented yet
    // @ManyToOne(() => LookupTableRevision)
    // @JoinColumn({ name: 'lookup_table_revision_id', foreignKeyConstraintName: 'FK_source_lookup_table_revision_id' })
    // lookupTableRevision: LookupTableRevision;

    @Column({ name: 'column_index', type: 'int', nullable: false })
    columnIndex: number;

    @Column({ name: 'csv_field', type: 'text' })
    csvField: string;

    @Column({ type: 'enum', enum: Object.values(SourceAction), nullable: false })
    action: string;

    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: Object.keys(SourceType),
        nullable: true
    })
    type: SourceType;
}
