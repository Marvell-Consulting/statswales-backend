import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToMany, JoinColumn } from 'typeorm';

import { User } from './user';
// eslint-disable-next-line import/no-cycle
import { RevisionEntity } from './revision';
// eslint-disable-next-line import/no-cycle
import { DatasetInfo } from './dataset_info';
// eslint-disable-next-line import/no-cycle
import { Dimension } from './dimension';

@Entity()
export class Dataset extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    creation_date: Date;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by' })
    created_by: User;

    @Column({ type: 'timestamp', nullable: true })
    live: Date;

    @Column({ type: 'timestamp', nullable: true })
    archive: Date;

    @OneToMany(() => RevisionEntity, (revision) => revision.dataset)
    revisions: RevisionEntity[];

    @OneToMany(() => DatasetInfo, (datasetInfo) => datasetInfo.dataset)
    datasetInfos: DatasetInfo[];

    @OneToMany(() => Dimension, (dimension) => dimension.dataset)
    dimensions: Dimension[];
}
