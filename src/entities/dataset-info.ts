import {
    Entity,
    PrimaryColumn,
    Column,
    BaseEntity,
    ManyToOne,
    JoinColumn,
    CreateDateColumn,
    UpdateDateColumn
} from 'typeorm';

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

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    // This column should be the same across all languages
    // If one is updated and the others aren't then mark as needing translation
    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
