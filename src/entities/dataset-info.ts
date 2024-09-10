import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';

@Entity()
export class DatasetInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'dataset_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_dataset_info_dataset_id_language'
    })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_dataset_info_dataset_id_language'
    })
    language: string;

    @Column({ type: 'text', nullable: true })
    title: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_info_dataset_id' })
    dataset: Promise<Dataset>;
}
