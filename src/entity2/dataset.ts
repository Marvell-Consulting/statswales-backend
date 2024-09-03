import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

import { Users } from './users';
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

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    creation_date: Date;

    @ManyToOne(() => Users)
    @JoinColumn({ name: 'created_by' })
    created_by: Promise<Users>;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', nullable: true })
    live: Date;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', nullable: true })
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
