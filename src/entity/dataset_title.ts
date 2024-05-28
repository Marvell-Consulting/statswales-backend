import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Dataset } from './dataset';

@Entity({ name: 'dataset_title' })
export class DatasetTitle extends BaseEntity {
    @PrimaryColumn({ name: 'dataset_id' })
    datasetId: string;

    @ManyToOne(() => Dataset, (dataset) => dataset.description, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'dataset_id' })
    dataset: Dataset;

    @Column({ nullable: false })
    title: string;

    @PrimaryColumn({ name: 'language_code' })
    languageCode: string;

    public static datasetTitleFromString(dataset: Dataset, title: string, language: string): DatasetTitle {
        const datasetTitle = new DatasetTitle();
        datasetTitle.dataset = dataset;
        datasetTitle.title = title;
        datasetTitle.languageCode = language;
        return datasetTitle;
    }
}
