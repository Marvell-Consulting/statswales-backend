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

import { RelatedLinkDTO } from '../../dtos/related-link-dto';
import { Designation } from '../../enums/designation';

import { Dataset } from './dataset';

@Entity({ name: 'dataset_info' })
export class DatasetInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'dataset_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_dataset_info_dataset_id_language'
    })
    id: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetInfo, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_info_dataset_id' })
    dataset: Dataset;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_dataset_info_dataset_id_language'
    })
    language: string;

    @Column({ type: 'text', nullable: true })
    title?: string;

    @Column({ type: 'text', nullable: true })
    description?: string;

    @Column({ type: 'text', nullable: true })
    collection?: string;

    @Column({ type: 'text', nullable: true })
    quality?: string;

    @Column({ type: 'boolean', name: 'rounding_applied', nullable: true })
    roundingApplied?: boolean;

    @Column({ type: 'text', name: 'rounding_description', nullable: true })
    roundingDescription?: string;

    @Column({ type: 'jsonb', name: 'related_links', nullable: true })
    relatedLinks?: RelatedLinkDTO[];

    @Column({ type: 'text', name: 'update_frequency', nullable: true })
    updateFrequency?: string; // in ISO 8601 duration format, e.g. P1Y = every year

    @Column({ type: 'enum', enum: Object.values(Designation), nullable: true })
    designation?: Designation;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    // This column should be the same across all languages
    // If one is updated and the others aren't then mark as needing translation
    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
