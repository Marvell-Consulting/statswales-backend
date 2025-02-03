import { DataTableDescription } from '../entities/dataset/data-table-description';

export class DataTableDescriptionDto {
    column_name: string;
    column_index: number;
    column_datatype: string;
    fact_table_column_name: string;

    static fromDataTableDescription(dataTableDescription: DataTableDescription): DataTableDescriptionDto {
        const dataTableDescriptionDto = new DataTableDescriptionDto();
        dataTableDescriptionDto.column_name = dataTableDescription.columnName;
        dataTableDescriptionDto.column_index = dataTableDescription.columnIndex;
        dataTableDescriptionDto.column_datatype = dataTableDescription.columnDatatype;
        dataTableDescriptionDto.fact_table_column_name = dataTableDescription.factTableColumn;
        return dataTableDescriptionDto;
    }
}
