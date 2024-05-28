/* eslint-disable import/no-cycle */
import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, OneToMany, ManyToOne, JoinColumn } from 'typeorm';

import { dbManager } from '../app';

import { Dataset } from './dataset';
import { ColumnTitle } from './column_title';

@Entity({ name: 'dataset_column' })
export class DatasetColumn extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @Column({ nullable: false })
    csvTitle: string;

    @Column({ nullable: true })
    type: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.columns, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @OneToMany(() => ColumnTitle, (columnTitle) => columnTitle.datasetColumn, {
        cascade: true
    })
    @JoinColumn([{ name: 'dataset_id' }, { name: 'language_code' }])
    title: Promise<ColumnTitle[]>;

    public addTitleByString(title: string, languague: string) {
        dbManager.getEntityManager().save(ColumnTitle.createColumnFromString(this, title, languague));
    }

    public addTitle(title: ColumnTitle) {
        dbManager.getEntityManager().save(title);
    }
}
