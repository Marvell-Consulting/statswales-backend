import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn, RelationId } from 'typeorm';

import { SourceAction } from '../../enums/source-action';
import { SourceType } from '../../enums/source-type';

import { Dimension } from './dimension';
import { FileImport } from './file-import';
import { Revision } from './revision';

@Entity({ name: 'source' })
export class Source extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_source_id' })
    id: string;

    @ManyToOne(() => Dimension, { nullable: true, onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dimension_id', foreignKeyConstraintName: 'FK_source_dimension_id' })
    dimension: Dimension;

    @Column({ name: 'dimension_id' })
    dimensionId: string;

    @ManyToOne(() => FileImport, (importEntity) => importEntity.sources, {
        nullable: false,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id', foreignKeyConstraintName: 'FK_source_import_id' })
    import: FileImport;

    @Column({ name: 'import_id' })
    importId: string;

    @ManyToOne(() => Revision, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_source_revision_id' })
    revision: Revision;

    @Column({ name: 'revision_id' })
    revisionId: string;

    // Not implemented yet
    // @ManyToOne(() => LookupTableRevision)
    // @JoinColumn({ name: 'lookup_table_revision_id', foreignKeyConstraintName: 'FK_source_lookup_table_revision_id' })
    // lookupTableRevision: LookupTableRevision;

    @Column({ name: 'column_index', type: 'int', nullable: false })
    columnIndex: number;

    @Column({ name: 'csv_field', type: 'text' })
    csvField: string;

    @Column({ type: 'enum', enum: Object.values(SourceAction), nullable: true })
    action?: SourceAction;

    @Column({ type: 'enum', enum: Object.values(SourceType), nullable: true })
    type?: SourceType;
}
