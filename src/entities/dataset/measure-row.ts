import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { DisplayType } from '../../enums/display-type';

import { Measure } from './measure';

@Entity({ name: 'measure_rows' })
export class MeasureRow extends BaseEntity {
    @PrimaryColumn({
        name: 'measure_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_measure_row_measure_id_language_reference'
    })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_measure_row_measure_id_language_reference'
    })
    language: string;

    @PrimaryColumn({
        name: 'reference',
        type: 'text',
        primaryKeyConstraintName: 'PK_measure_row_measure_id_language_reference'
    })
    reference: string;

    @Column({ name: 'format', type: 'enum', enum: Object.values(DisplayType), nullable: false })
    format: string;

    @Column({ name: 'decimal', type: 'integer', nullable: true })
    decimal: number | null;

    @Column({ type: 'varchar' })
    description: string;

    @Column({ name: 'sort_order', type: 'int', nullable: true })
    sortOrder: number | null;

    @Column({ type: 'text', nullable: true })
    notes: string | null;

    @Column({ name: 'hierarchy', type: 'varchar', nullable: true })
    hierarchy: string | null;

    @ManyToOne(() => Measure, (measure) => measure.measureTable, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'measure_id', foreignKeyConstraintName: 'FK_measure_row_measure_id' })
    measure: Measure;

    @Column({ name: 'measure_type', type: 'varchar', nullable: true })
    measureType: string | null;
}
