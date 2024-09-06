import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';
// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
// eslint-disable-next-line import/no-cycle
import { DimensionInfo } from './dimension_info';
import { Source } from './source';
import { DimensionType } from './dimension_type';

@Entity()
export class Dimension extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => Dataset, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Promise<Dataset>;

    // Replace with actual enum types
    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: Object.keys(DimensionType),
        nullable: false
    })
    type: DimensionType;

    @ManyToOne(() => Revision)
    @JoinColumn({ name: 'start_revision_id' })
    startRevision: Promise<Revision>;

    @ManyToOne(() => Revision, { nullable: true })
    @JoinColumn({ name: 'finish_revision_id' })
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
