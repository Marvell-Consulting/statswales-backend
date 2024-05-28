/* eslint-disable import/no-cycle */
import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { Dataset } from './dataset';

@Entity({ name: 'dataset_description' })
export class DatasetDescription extends BaseEntity {
    @PrimaryColumn({ name: 'dataset_id' })
    datasetID: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.description, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @Column({ nullable: false })
    description: string;

    @PrimaryColumn()
    @Column({ name: 'language_code' })
    languageCode: string;

    public static datasetDescriptionFromString(
        dataset: Dataset,
        description: string,
        language: string
    ): DatasetDescription {
        const datasetDescription = new DatasetDescription();
        datasetDescription.dataset = dataset;
        datasetDescription.description = description;
        datasetDescription.languageCode = language;
        return datasetDescription;
    }
}
