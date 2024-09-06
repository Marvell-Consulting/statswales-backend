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
// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
// eslint-disable-next-line import/no-cycle
import { DatasetInfo } from './dataset_info';
// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';

@Entity()
export class Dataset extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @CreateDateColumn()
    creation_date: Date;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by' })
    createdBy: Promise<User>;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamptz', nullable: true })
    live: Date;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamptz', nullable: true })
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
