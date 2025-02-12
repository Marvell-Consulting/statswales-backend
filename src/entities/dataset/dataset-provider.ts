import {
    BaseEntity,
    Column,
    CreateDateColumn,
    Entity,
    Index,
    JoinColumn,
    ManyToOne,
    PrimaryGeneratedColumn
} from 'typeorm';

import { Provider } from './provider';
import { Dataset } from './dataset';
import { ProviderSource } from './provider-source';

@Entity({ name: 'dataset_provider' })
export class DatasetProvider extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_dataset_provider_id' })
    id: string;

    @Column({ type: 'uuid', name: 'group_id' })
    groupId: string; // dataset providers can be in multiple languages - use this id to group them

    @Index('IDX_dataset_provider_dataset_id')
    @Column({ type: 'uuid', name: 'dataset_id' })
    datasetId: string;

    @Column({ type: 'varchar', length: 5 })
    language: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_provider_dataset_id' })
    dataset: Dataset;

    @Column({ type: 'uuid', name: 'provider_id' })
    providerId: string;

    @Index('IDX_dataset_provider_provider_id_language', ['provider_id', 'language'])
    @ManyToOne(() => Provider, (provider) => provider.datasetProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn([
        {
            name: 'provider_id',
            referencedColumnName: 'id',
            foreignKeyConstraintName: 'FK_dataset_provider_provider_id_language'
        },
        {
            name: 'language',
            referencedColumnName: 'language',
            foreignKeyConstraintName: 'FK_dataset_provider_provider_id_language'
        }
    ])
    provider: Provider;

    @Column({ type: 'uuid', name: 'provider_source_id', nullable: true })
    providerSourceId?: string;

    @Index('IDX_dataset_provider_provider_source_id_language', ['provider_source_id', 'language'])
    @ManyToOne(() => ProviderSource, (providerSource) => providerSource.datasetProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn([
        {
            name: 'provider_source_id',
            referencedColumnName: 'id',
            foreignKeyConstraintName: 'FK_dataset_provider_provider_source_id_language'
        },
        {
            name: 'language',
            referencedColumnName: 'language',
            foreignKeyConstraintName: 'FK_dataset_provider_provider_source_id_language'
        }
    ])
    providerSource: ProviderSource;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;
}
