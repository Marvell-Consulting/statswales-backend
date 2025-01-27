import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    JoinColumn,
    OneToMany,
    ManyToOne
} from 'typeorm';

import { User } from '../user/user';

import { RevisionInterface } from './revision.interface';
import { Dataset } from './dataset';
import { FactTable } from './fact-table';

@Entity({ name: 'revision', orderBy: { createdAt: 'ASC' } })
export class Revision extends BaseEntity implements RevisionInterface {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_revision_id' })
    id: string;

    @Column({ name: 'revision_index', type: 'int' })
    revisionIndex: number;

    @ManyToOne(() => Dataset, (dataset) => dataset.revisions, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_revision_dataset_id' })
    dataset: Dataset;

    @ManyToOne(() => Revision, { nullable: true, onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'previous_revision_id', foreignKeyConstraintName: 'FK_revision_previous_revision_id' })
    previousRevision: RevisionInterface;

    @Column({ name: 'online_cube_filename', type: 'varchar', length: 255, nullable: true })
    onlineCubeFilename: string;

    @OneToMany(() => FactTable, (factTable) => factTable.revision, { cascade: true })
    factTables: FactTable[];

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_revision_created_by' })
    createdBy: User;

    @Column({ name: 'approved_at', type: 'timestamptz', nullable: true })
    approvedAt: Date | null;

    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by', foreignKeyConstraintName: 'FK_revision_approved_by' })
    approvedBy: User | null;

    @Column({ name: 'publish_at', type: 'timestamptz', nullable: true })
    publishAt: Date;
}
