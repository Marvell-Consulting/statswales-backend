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

import { Revision } from './revision';

@Entity({ name: 'revision_metadata' })
export class RevisionMetadata extends BaseEntity {
    @PrimaryColumn({
        name: 'revision_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_revision_metadata_revision_id_language'
    })
    id: string;

    @ManyToOne(() => Revision, (revision) => revision.metadata, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_revision_metadata_revision_id' })
    revision: Revision;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_revision_metadata_revision_id_language'
    })
    language: string;

    @Column({ type: 'text', nullable: true })
    title?: string;

    @Column({ type: 'text', nullable: true })
    summary?: string;

    @Column({ type: 'text', nullable: true })
    collection?: string;

    @Column({ type: 'text', nullable: true })
    quality?: string;

    @Column({ type: 'text', name: 'rounding_description', nullable: true })
    roundingDescription?: string;

    @Column({ name: 'reason', type: 'text', nullable: true })
    reason?: string | null;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    // This column should be the same across all languages
    // If one is updated and the others aren't then mark as needing translation
    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
