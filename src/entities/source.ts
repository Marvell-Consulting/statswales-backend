import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';
// eslint-disable-next-line import/no-cycle
import { FileImport } from './import_file';
// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
import { SourceType } from './source_type';

@Entity()
export class Source extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => Dimension, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dimension_id' })
    dimension: Promise<Dimension>;

    @ManyToOne(() => FileImport, (importEntity) => importEntity.sources, {
        nullable: false,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id' })
    import: Promise<FileImport>;

    @ManyToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id' })
    revision: Promise<Revision>;

    // Not implemented yet
    // @ManyToOne(() => LookupTableRevision)
    // @JoinColumn({ name: 'lookup_table_revision_id' })
    // lookupTableRevision: LookupTableRevision;

    @Column({ name: 'column_index', type: 'int', nullable: false })
    columnIndex: number;

    @Column({ name: 'csv_field', type: 'text' })
    csvField: string;

    // Replace with actual enum types
    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: ['unknown', 'create', 'append', 'truncate-then-load', 'ignore'],
        nullable: false
    })
    action: string;

    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: Object.keys(SourceType),
        nullable: true
    })
    type: SourceType;
}
