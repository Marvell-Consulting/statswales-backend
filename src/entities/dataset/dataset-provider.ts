import { BaseEntity, Column, CreateDateColumn, Entity, JoinColumn, ManyToOne, PrimaryColumn } from 'typeorm';

import { Provider } from './provider';
import { Dataset } from './dataset';
import { ProviderSource } from './provider-source';

@Entity({ name: 'dataset_provider' })
export class DatasetProvider extends BaseEntity {
    @PrimaryColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_dataset_provider_id' })
    id: string;

    @Column({ type: 'uuid', name: 'dataset_id' })
    datasetId: string;

    @Column({ type: 'varchar', length: 5 })
    language: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datasetProviders)
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_provider_dataset_id' })
    dataset: Dataset;

    @Column({ type: 'uuid', name: 'provider_id' })
    providerId: string;

    @ManyToOne(() => Provider, (provider) => provider.datasetProviders)
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
    providerSourceId: string;

    @ManyToOne(() => ProviderSource, (providerSource) => providerSource.datasetProviders)
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
