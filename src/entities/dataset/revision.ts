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
import { Source } from './source';
import { FileImport } from './file-import';

@Entity({ name: 'revision', orderBy: { createdAt: 'ASC' } })
export class Revision extends BaseEntity implements RevisionInterface {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_revision_id' })
    id: string;

    @Column({ name: 'revision_index', type: 'int' })
    revisionIndex: number;

    @ManyToOne(() => Dataset, (dataset) => dataset.revisions, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_revision_dataset_id' })
    dataset: Promise<Dataset>;

    @ManyToOne(() => Revision, {
        nullable: true,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'previous_revision_id', foreignKeyConstraintName: 'FK_revision_previous_revision_id' })
    previousRevision: Promise<RevisionInterface>;

    @Column({ name: 'online_cube_filename', type: 'varchar', length: 255, nullable: true })
    onlineCubeFilename: string;

    @OneToMany(() => Source, (source) => source.revision, {
        cascade: true
    })
    sources: Promise<Source[]>;

    @OneToMany(() => FileImport, (importEntity) => importEntity.revision, {
        cascade: true
    })
    imports: Promise<FileImport[]>;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_revision_created_by' })
    createdBy: Promise<User>;

    @Column({ name: 'approved_at', type: 'timestamptz', nullable: true })
    approvedAt: Date;

    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by', foreignKeyConstraintName: 'FK_revision_approved_by' })
    approvedBy: Promise<User>;

    @Column({ name: 'publish_at', type: 'timestamptz', nullable: true })
    publishAt: Date;
}
