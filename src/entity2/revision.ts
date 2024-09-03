import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, JoinColumn, OneToMany, ManyToOne } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';
import { Users } from './users';
// eslint-disable-next-line import/no-cycle
import { Import } from './import';

interface RevisionInterface {
    id: string;
    revision_index: number;
    dataset: Promise<Dataset>;
    creation_date: Date;
    previous_revision: Promise<RevisionInterface>;
    online_cube_filename: string;
    publish_date: Date;
    approval_date: Date;
    approved_by: Promise<Users>;
    created_by: Promise<Users>;
    imports: Promise<Import[]>;
}

@Entity()
export class Revision extends BaseEntity implements RevisionInterface {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ type: 'int' })
    revision_index: number;

    @ManyToOne(() => Dataset, (dataset) => dataset.revisions, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Promise<Dataset>;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    creation_date: Date;

    @ManyToOne(() => Revision, {
        nullable: true,
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'previous_revision_id' })
    previous_revision: Promise<RevisionInterface>;

    @Column({ type: 'varchar', length: 255, nullable: true })
    online_cube_filename: string;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', nullable: true })
    publish_date: Date;

    @Column({ type: process.env.NODE_ENV === 'test' ? 'datetime' : 'timestamp', nullable: true })
    approval_date: Date;

    @OneToMany(() => Import, (importEntity) => importEntity.revision, {
        cascade: true
    })
    imports: Promise<Import[]>;

    @ManyToOne(() => Users, { nullable: true })
    @JoinColumn({ name: 'approved_by' })
    approved_by: Promise<Users>;

    @ManyToOne(() => Users)
    @JoinColumn({ name: 'created_by' })
    created_by: Promise<Users>;
}
