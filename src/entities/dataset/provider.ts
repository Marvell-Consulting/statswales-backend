import { BaseEntity, Column, Entity, OneToMany, PrimaryColumn } from 'typeorm';

import { ProviderSource } from './provider-source';
import { DatasetProvider } from './dataset-provider';

@Entity({ name: 'provider' })
export class Provider extends BaseEntity {
    @PrimaryColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_provider_id_language' })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_provider_id_language'
    })
    language: string;

    @Column({ type: 'text', nullable: false })
    name: string;

    @OneToMany(() => ProviderSource, (source) => source.provider)
    sources?: ProviderSource[];

    @OneToMany(() => DatasetProvider, (datasetProvider) => datasetProvider.provider, { cascade: true })
    datasetProviders?: DatasetProvider[];
}
