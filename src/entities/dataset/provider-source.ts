import { BaseEntity, Column, Entity, Index, JoinColumn, ManyToOne, OneToMany, PrimaryColumn } from 'typeorm';

import { Provider } from './provider';
import { RevisionProvider } from './revision-provider';

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

  @Column({ type: 'int', name: 'sw2_id', nullable: true })
  sw2_id?: number; // provider id from SW2 - might need this for migration but otherwise unused

  @Column({ type: 'text', nullable: false })
  name: string;

  @Column({ type: 'uuid', name: 'provider_id' })
  providerId: string;

  @Index('IDX_provider_source_provider_id_language', ['provider_id', 'language'])
  @ManyToOne(() => Provider, (provider) => provider.sources, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete',
    cascade: true
  })
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

  @OneToMany(() => RevisionProvider, (revisionProvider) => revisionProvider.revision, { cascade: true })
  revisionProviders: RevisionProvider[];
}
