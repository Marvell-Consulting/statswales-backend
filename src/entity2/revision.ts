import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, JoinColumn, ManyToOne } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';
import { User } from './user';

interface Revision {
    id: string;
    revision_index: number;
    dataset: Dataset;
    creation_date: Date;
    previous_revision: Revision;
    online_cube_filename: string;
    publish_date: Date;
    approval_date: Date;
    approved_by: User;
    created_by: User;
}

@Entity()
export class RevisionEntity extends BaseEntity implements Revision {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'int' })
    revision_index: number;

    @ManyToOne(() => Dataset, (dataset) => dataset.revisions)
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    creation_date: Date;

    @ManyToOne(() => RevisionEntity, { nullable: true })
    @JoinColumn({ name: 'previous_revision_id' })
    previous_revision: Revision;

    @Column({ type: 'varchar', length: 255, nullable: true })
    online_cube_filename: string;

    @Column({ type: 'timestamp', nullable: true })
    publish_date: Date;

    @Column({ type: 'timestamp', nullable: true })
    approval_date: Date;

    @ManyToOne(() => User, { nullable: true })
    @JoinColumn({ name: 'approved_by' })
    approved_by: User;

    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by' })
    created_by: User;
}
