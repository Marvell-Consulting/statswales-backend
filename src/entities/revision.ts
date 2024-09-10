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
import { RevisionInterface } from '../dtos/revision';

import { Dataset } from './dataset';
import { User } from './user';
// eslint-disable-next-line import/no-cycle
import { Import } from './import';

@Entity()
export class Revision extends BaseEntity implements RevisionInterface {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_revision_id' })
    id: string;

    @Column({ type: 'int' })
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

    @OneToMany(() => Import, (importEntity) => importEntity.revision, { cascade: true })
    imports: Promise<Import[]>;

    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by', foreignKeyConstraintName: 'FK_revision_approved_by' })
    approvedBy: Promise<User>;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_revision_created_by' })
    createdBy: Promise<User>;
}
