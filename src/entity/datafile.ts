import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';

@Entity({ name: 'datafiles' })
export class Datafile extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ nullable: false })
    sha256hash: string;

    @Column({ name: 'draft', default: true })
    draft: boolean;

    @ManyToOne(() => Dataset, (dataset) => dataset.datafiles, { onDelete: 'CASCADE', eager: true })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @CreateDateColumn({ name: 'creation_date' })
    creationDate: Date;

    @Column({ name: 'created_by', nullable: true })
    createdBy: string;

    public static createDatafile(dataset: Dataset, hash: string, user: string): Datafile {
        const datafile = new Datafile();
        datafile.dataset = dataset;
        datafile.draft = true;
        datafile.sha256hash = hash;
        datafile.createdBy = user;
        datafile.creationDate = new Date(Date.now());
        return datafile;
    }
}
