import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, OneToOne, JoinColumn, OneToMany } from 'typeorm';

import { Dataset } from './dataset';
import { LookupTable } from './lookup-table';
import { MeasureItem } from './measure-item';
import { MeasureMetadata } from './measure-metadata';

/*
    Describes what's being measured in the cube and how to display cube values
 */
@Entity({ name: 'measure' })
export class Measure extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_measure_id' })
    id: string;

    @OneToOne(() => Dataset, (dataset) => dataset.measure, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_measure_dataset_id' })
    dataset: Dataset;

    @OneToOne(() => LookupTable, { cascade: true, onDelete: 'CASCADE' })
    @JoinColumn({ name: 'lookup_table_id', foreignKeyConstraintName: 'FK_measure_lookup_table_id' })
    lookupTable: LookupTable | null;

    @Column({ name: 'fact_table_column', type: 'varchar' })
    factTableColumn: string;

    @Column({ name: 'join_column', type: 'varchar', nullable: true })
    joinColumn: string | null; // <-- Tells you how to join measure to the fact table

    @Column({ name: 'extractor', type: 'jsonb', nullable: true })
    extractor: object | null;

    @OneToMany(() => MeasureItem, (measureInfo) => measureInfo.measure, { cascade: true })
    measureTable: MeasureItem[] | null;

    @OneToMany(() => MeasureMetadata, (measureMetadata) => measureMetadata.measure, { cascade: true })
    metadata: MeasureMetadata[];
}
