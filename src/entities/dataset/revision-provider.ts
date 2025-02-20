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
import { ProviderSource } from './provider-source';
import { Revision } from './revision';

@Entity({ name: 'revision_provider' })
export class RevisionProvider extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { name: 'id', primaryKeyConstraintName: 'PK_revision_provider_id' })
    id: string;

    @Column({ type: 'uuid', name: 'group_id' })
    groupId: string; // revision providers can be in multiple languages - use this id to group them

    @Index('IDX_revision_provider_revision_id')
    @Column({ type: 'uuid', name: 'revision_id' })
    revisionId: string;

    @Column({ type: 'varchar', length: 5 })
    language: string;

    @ManyToOne(() => Revision, (revision) => revision.revisionProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_revision_provider_revision_id' })
    revision: Revision;

    @Column({ type: 'uuid', name: 'provider_id' })
    providerId: string;

    @Index('IDX_revision_provider_provider_id_language', ['provider_id', 'language'])
    @ManyToOne(() => Provider, (provider) => provider.revisionProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn([
        {
            name: 'provider_id',
            referencedColumnName: 'id',
            foreignKeyConstraintName: 'FK_revision_provider_provider_id_language'
        },
        {
            name: 'language',
            referencedColumnName: 'language',
            foreignKeyConstraintName: 'FK_revision_provider_provider_id_language'
        }
    ])
    provider: Provider;

    @Column({ type: 'uuid', name: 'provider_source_id', nullable: true })
    providerSourceId?: string;

    @Index('IDX_revision_provider_provider_source_id_language', ['provider_source_id', 'language'])
    @ManyToOne(() => ProviderSource, (providerSource) => providerSource.revisionProviders, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn([
        {
            name: 'provider_source_id',
            referencedColumnName: 'id',
            foreignKeyConstraintName: 'FK_revision_provider_provider_source_id_language'
        },
        {
            name: 'language',
            referencedColumnName: 'language',
            foreignKeyConstraintName: 'FK_revision_provider_provider_source_id_language'
        }
    ])
    providerSource: ProviderSource;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;
}
