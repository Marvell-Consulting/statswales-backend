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

// eslint-disable-next-line import/no-cycle
import { RevisionInterface } from './revision.interface';
import { Dataset } from './dataset';
import { User } from './user';
// eslint-disable-next-line import/no-cycle
import { Source } from './source';
// eslint-disable-next-line import/no-cycle
import { FileImport } from './import-file';

@Entity({ name: 'revision', orderBy: { creationDate: 'ASC' } })
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

    @CreateDateColumn({ name: 'creation_date' })
    creationDate: Date;

    @ManyToOne(() => Revision, {
        nullable: true,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'previous_revision_id', foreignKeyConstraintName: 'FK_revision_previous_revision_id' })
    previousRevision: Promise<RevisionInterface>;

    @Column({ name: 'online_cube_filename', type: 'varchar', length: 255, nullable: true })
    onlineCubeFilename: string;

    @Column({ name: 'publish_date', type: 'timestamptz', nullable: true })
    publishDate: Date;

    @Column({ name: 'approval_date', type: 'timestamptz', nullable: true })
    approvalDate: Date;

    @OneToMany(() => Source, (source) => source.revision, {
        cascade: true
    })
    sources: Promise<Source[]>;

    @OneToMany(() => FileImport, (importEntity) => importEntity.revision, {
        cascade: true
    })
    imports: Promise<FileImport[]>;

    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by', foreignKeyConstraintName: 'FK_revision_approved_by' })
    approvedBy: Promise<User>;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_revision_created_by' })
    createdBy: Promise<User>;
}
