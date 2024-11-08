import { BaseEntity, Column, Entity, JoinColumn, ManyToOne, OneToMany, PrimaryColumn } from 'typeorm';

import { Provider } from './provider';
import { DatasetProvider } from './dataset-provider';

@Entity({ name: 'provider_source' })
export class ProviderSource extends BaseEntity {
    @PrimaryColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_provider_source_id_language' })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_provider_source_id_language'
    })
    language: string;

    @Column({ type: 'text', nullable: false })
    name: string;

    @Column({ type: 'uuid', name: 'provider_id' })
    providerId: string;

    @ManyToOne(() => Provider, (provider) => provider.sources, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn([
        {
            name: 'provider_id',
            referencedColumnName: 'id',
            foreignKeyConstraintName: 'FK_provider_source_provider_id'
        },
        {
            name: 'language',
            referencedColumnName: 'language',
            foreignKeyConstraintName: 'FK_provider_source_provider_id'
        }
    ])
    provider: Provider;

    @OneToMany(() => DatasetProvider, (datasetProvider) => datasetProvider.dataset, { cascade: true })
    datasetProviders: DatasetProvider[];
}
