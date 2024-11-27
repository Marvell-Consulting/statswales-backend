import { FactTableColumnType } from '../enums/fact-table-column-type';

export class SourceAssignmentDTO {
    columnIndex: number;
    columnName: string;
    columnType: FactTableColumnType;
}
