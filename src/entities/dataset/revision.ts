import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    JoinColumn,
    ManyToOne,
    OneToOne,
    Index,
    OneToMany,
    UpdateDateColumn
} from 'typeorm';

import { User } from '../user/user';
import { RevisionTask } from '../../interfaces/revision-task';
import { RelatedLink } from '../../dtos/related-link-dto';
import { Designation } from '../../enums/designation';

import { Dataset } from './dataset';
import { DataTable } from './data-table';
import { RevisionMetadata } from './revision-metadata';
import { RevisionProvider } from './revision-provider';
import { RevisionTopic } from './revision-topic';

@Entity({ name: 'revision', orderBy: { createdAt: 'ASC' } })
export class Revision extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_revision_id' })
    id: string;

    @Column({ name: 'dataset_id' })
    datasetId: string;

    @Index('IDX_revison_dataset_id')
    @ManyToOne(() => Dataset, (dataset) => dataset.revisions, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_revision_dataset_id' })
    dataset: Dataset;

    @Column({ name: 'revision_index', type: 'int', nullable: false })
    revisionIndex: number;

    @Column({ name: 'previous_revision_id', nullable: true })
    previousRevisionId?: string;

    @Index('IDX_revison_previous_revision_id')
    @ManyToOne(() => Revision, { nullable: true, onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'previous_revision_id', foreignKeyConstraintName: 'FK_revision_previous_revision_id' })
    previousRevision: Revision | null;

    @Column({ name: 'online_cube_filename', type: 'varchar', length: 255, nullable: true })
    onlineCubeFilename: string | null;

    @Index('IDX_revison_data_table_id')
    @Column({ name: 'data_table_id', nullable: true })
    dataTableId?: string;

    @OneToOne(() => DataTable, (dataTable) => dataTable.revision, { cascade: true, onDelete: 'SET NULL' })
    @JoinColumn({ name: 'data_table_id', foreignKeyConstraintName: 'FK_revision_data_table_id' })
    dataTable: DataTable | null;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @Index('IDX_revison_created_by')
    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_revision_created_by' })
    createdBy: User;

    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;

    @Column({ name: 'approved_at', type: 'timestamptz', nullable: true })
    approvedAt: Date | null;

    @Index('IDX_revison_approved_by')
    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by', foreignKeyConstraintName: 'FK_revision_approved_by' })
    approvedBy: User | null;

    @Column({ name: 'publish_at', type: 'timestamptz', nullable: true })
    publishAt: Date;

    @Column({ name: 'tasks', type: 'jsonb', nullable: true })
    tasks: RevisionTask;

    @Column({ type: 'boolean', name: 'rounding_applied', nullable: true })
    roundingApplied?: boolean;

    @Column({ type: 'text', name: 'update_frequency', nullable: true })
    updateFrequency?: string; // in ISO 8601 duration format, e.g. P1Y = every year

    @Column({ type: 'text', nullable: true })
    designation?: Designation;

    @Column({ type: 'jsonb', name: 'related_links', nullable: true })
    relatedLinks?: RelatedLink[];

    @OneToMany(() => RevisionMetadata, (metadata) => metadata.revision, { cascade: true })
    metadata: RevisionMetadata[];

    @OneToMany(() => RevisionProvider, (revisionProvider) => revisionProvider.revision, { cascade: true })
    revisionProviders: RevisionProvider[];

    @OneToMany(() => RevisionTopic, (revisionTopic) => revisionTopic.revision, { cascade: true })
    revisionTopics: RevisionTopic[];
}
