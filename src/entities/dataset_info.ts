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

import { Dataset } from './dataset';

@Entity()
export class DatasetInfo extends BaseEntity {
    @PrimaryColumn({ name: 'dataset_id', type: process.env.NODE_ENV === 'test' ? 'text' : 'uuid' })
    id: string;

    @PrimaryColumn({ name: 'language', type: 'varchar', length: 5 })
    language: string;

    @Column({ type: 'text', nullable: true })
    title: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Promise<Dataset>;

    @CreateDateColumn({ name: 'created_at' })
    createdAt: Date;

    // This column should be the same across all languages
    // If one is updated and the others aren't then mark as needing translation
    @UpdateDateColumn({ name: 'updated_at', nullable: true })
    updatedAt: Date;
}
