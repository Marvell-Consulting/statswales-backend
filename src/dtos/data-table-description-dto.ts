import { DataTableDescription } from '../entities/dataset/data-table-description';

export class DataTableDescriptionDto {
    data_table_id: string;
    column_name: string;
    column_index: number;
    fact_table_column_name: string;

    static fromDataTableDescription(dataTableDescription: DataTableDescription): DataTableDescriptionDto {
        const dataTableDescriptionDto = new DataTableDescriptionDto();
        dataTableDescriptionDto.data_table_id = dataTableDescription.id;
        dataTableDescriptionDto.column_name = dataTableDescription.columnName;
        dataTableDescriptionDto.column_index = dataTableDescription.columnIndex;
        dataTableDescriptionDto.fact_table_column_name = dataTableDescription.factTableColumn;
        return dataTableDescriptionDto;
    }
}
