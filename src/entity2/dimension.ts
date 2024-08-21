import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';
// eslint-disable-next-line import/no-cycle
import { RevisionEntity } from './revision';
// eslint-disable-next-line import/no-cycle
import { DimensionInfo } from './dimension_info';
import { Source } from './source';

@Entity()
export class Dimension extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => Dataset)
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    // Replace with actual enum types
    @Column({ type: 'enum', enum: ['type1', 'type2'], nullable: false })
    type: string;

    @ManyToOne(() => RevisionEntity)
    @JoinColumn({ name: 'start_revision_id' })
    start_revision: RevisionEntity;

    @ManyToOne(() => RevisionEntity, { nullable: true })
    @JoinColumn({ name: 'finish_revision_id' })
    finish_revision: RevisionEntity;

    @Column({ type: 'text', nullable: true })
    validator: string;

    @OneToMany(() => DimensionInfo, (dimensionInfo) => dimensionInfo.dimension)
    dimensionInfos: DimensionInfo[];

    @OneToMany(() => Source, (source) => source.dimension)
    sources: Source[];
}
