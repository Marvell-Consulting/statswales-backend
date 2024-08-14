import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';

@Entity()
export class DatasetInfo extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'varchar', length: 5, nullable: true })
    language: string;

    @Column({ type: 'text', nullable: true })
    title: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetInfos)
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;
}
