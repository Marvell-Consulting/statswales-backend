import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { Dimension } from './dimension';

@Entity()
export class DimensionInfo extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'varchar', length: 5, nullable: true })
    language: string;

    @Column({ type: 'text' })
    name: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => Dimension, (dimension) => dimension.dimensionInfos)
    @JoinColumn({ name: 'dimension_id' })
    dimension: Dimension;
}
