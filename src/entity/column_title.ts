import { Entity, Column, PrimaryColumn, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { DatasetColumn } from './dataset_column';

@Entity({ name: 'column_title' })
export class ColumnTitle extends BaseEntity {
    @PrimaryColumn({ name: 'dataset_column_id' })
    datasetColumnId: string;

    @ManyToOne(() => DatasetColumn, (datasetColumn) => datasetColumn.title, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'dataset_column_id' })
    datasetColumn: DatasetColumn;

    @Column({ nullable: false })
    title: string;

    @PrimaryColumn({ name: 'language_code' })
    languageCode: string;

    public static createColumnFromString(column: DatasetColumn, title: string, language: string) {
        const columnTitle = new ColumnTitle();
        columnTitle.title = title;
        columnTitle.languageCode = language;
        columnTitle.datasetColumn = column;
        return columnTitle;
    }
}
