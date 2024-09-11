import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { SourceAction } from '../enums/source-action';
import { SourceType } from '../enums/source-type';

// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';
// eslint-disable-next-line import/no-cycle
import { FileImport } from './file-import';
// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';

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

    @ManyToOne(() => FileImport, (importEntity) => importEntity.sources, {
        nullable: false,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id', foreignKeyConstraintName: 'FK_source_import_id' })
    import: Promise<FileImport>;

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

<   @Column({ type: 'enum', enum: Object.values(SourceAction), nullable: true })
    action: SourceAction;

    @Column({ type: 'enum', enum: Object.values(SourceType), nullable: true })
    type: SourceType;
}
