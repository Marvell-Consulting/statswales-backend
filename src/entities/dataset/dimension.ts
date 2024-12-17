import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    BaseEntity,
    ManyToOne,
    OneToMany,
    JoinColumn,
    OneToOne
} from 'typeorm';

import { DimensionType } from '../../enums/dimension-type';

import { Dataset } from './dataset';
import { DimensionInfo } from './dimension-info';
import { LookupTable } from './lookup-table';

@Entity({ name: 'dimension' })
export class Dimension extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_dimension_id' })
    id: string;

    @ManyToOne(() => Dataset, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dimension_dataset_id' })
    dataset: Dataset;

    @Column({ type: 'enum', enum: Object.values(DimensionType), nullable: false })
    type: DimensionType;

    @Column({ type: 'jsonb', nullable: true })
    extractor: object | null;

    @Column({ name: 'join_column', type: 'varchar', nullable: true })
    joinColumn: string | null; // <-- Tells you have to join the dimension to the fact_table

    @Column({ name: 'fact_table_column', type: 'varchar', nullable: false })
    factTableColumn: string; // <-- Tells you which column in the fact table you're joining to

    @Column({ name: 'is_slice_dimension', type: 'boolean', default: false })
    isSliceDimension: boolean;

    @OneToOne(() => LookupTable, (lookupTable) => lookupTable.dimension, { cascade: true })
    @JoinColumn({
        name: 'lookup_table_id',
        foreignKeyConstraintName: 'FK_dimension_lookup_table_id_lookup_table_dimension_id'
    })
    lookupTable: LookupTable | null;

    @OneToMany(() => DimensionInfo, (dimensionInfo) => dimensionInfo.dimension, { cascade: true })
    dimensionInfo: DimensionInfo[];
}
