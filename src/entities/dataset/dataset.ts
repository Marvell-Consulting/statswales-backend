import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    ManyToOne,
    OneToMany,
    JoinColumn
} from 'typeorm';

import { User } from './user';
import { Revision } from './revision';
import { DatasetInfo } from './dataset-info';
import { Dimension } from './dimension';

@Entity({ name: 'dataset' })
export class Dataset extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_dataset_id' })
    id: string;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_dataset_created_by' })
    createdBy: Promise<User>;

    @Column({ type: 'timestamptz', nullable: true })
    live: Date;

    @Column({ type: 'timestamptz', nullable: true })
    archive: Date;

    @OneToMany(() => DatasetInfo, (datasetInfo) => datasetInfo.dataset, {
        cascade: true
    })
    datasetInfo: Promise<DatasetInfo[]>;

    @OneToMany(() => Dimension, (dimension) => dimension.dataset, {
        cascade: true
    })
    dimensions: Promise<Dimension[]>;

    @OneToMany(() => Revision, (revision) => revision.dataset, {
        cascade: true
    })
    revisions: Promise<Revision[]>;
}
