import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

import { DimensionType } from '../../enums/dimension-type';

import { Dataset } from './dataset';
import { Revision } from './revision';
import { DimensionInfo } from './dimension-info';
import { Source } from './source';

@Entity({ name: 'dimension' })
export class Dimension extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_dimension_id' })
    id: string;

    @ManyToOne(() => Dataset, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dimension_dataset_id' })
    dataset: Promise<Dataset>;

    @Column({ type: 'enum', enum: Object.values(DimensionType), nullable: false })
    type: DimensionType;

    @ManyToOne(() => Revision, { cascade: true, onDelete: 'CASCADE' })
    @JoinColumn({ name: 'start_revision_id', foreignKeyConstraintName: 'FK_dimension_start_revision_id' })
    startRevision: Promise<Revision>;

    @ManyToOne(() => Revision, { cascade: true, onDelete: 'CASCADE', nullable: true })
    @JoinColumn({ name: 'finish_revision_id', foreignKeyConstraintName: 'FK_dimension_finish_revision_id' })
    finishRevision: Promise<Revision>;

    @Column({ type: 'text', nullable: true })
    validator: string;

    @OneToMany(() => DimensionInfo, (dimensionInfo) => dimensionInfo.dimension, {
        cascade: true
    })
    dimensionInfo: Promise<DimensionInfo[]>;

    @OneToMany(() => Source, (source) => source.dimension, {
        cascade: true
    })
    sources: Promise<Source[]>;
}
