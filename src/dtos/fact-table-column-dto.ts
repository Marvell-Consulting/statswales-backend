import { FactTable } from '../entities/dataset/fact-table';

export class FactTableColumnDto {
    name: string;
    index: number;
    type: string;
    datatype: string;

    static fromFactTableColumn(factTableColumn: FactTable): FactTableColumnDto {
        const dto = new FactTableColumnDto();
        dto.name = factTableColumn.columnName;
        dto.index = factTableColumn.columnIndex;
        dto.type = factTableColumn.columnType;
        dto.datatype = factTableColumn.columnDatatype;
        return dto;
    }
}
