import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';

@Entity()
export class DimensionInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'dimension_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_dimension_info_dimension_id_language'
    })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_dimension_info_dimension_id_language'
    })
    language: string;

    @Column({ type: 'text' })
    name: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => Dimension, (dimension) => dimension.dimensionInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dimension_id', foreignKeyConstraintName: 'FK_dimension_info_dimension_id' })
    dimension: Promise<Dimension>;
}
