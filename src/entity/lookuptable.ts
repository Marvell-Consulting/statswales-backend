/* eslint-disable import/no-cycle */
import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { Dataset } from './dataset';
import { DatasetColumn } from './dataset_column';

@Entity({ name: 'lookuptable' })
export class LookupTable extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ nullable: false })
    sha256hash: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.datafiles, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @CreateDateColumn({ name: 'creation_date' })
    creationDate: Date;

    @Column({ name: 'created_by', nullable: true })
    createdBy: string;

    @CreateDateColumn({ name: 'last_modified' })
    lastModified: Date;

    @Column({ name: 'modified_by', nullable: true })
    modifiedBy: string;

    @ManyToOne(() => DatasetColumn, (datasetColumn) => datasetColumn.id, { onDelete: 'CASCADE' })
    datasetColumn: DatasetColumn;
}
