import { FactTableColumnType } from '../enums/fact-table-column-type';

export class SourceAssignmentDTO {
    column_index: number;
    column_name: string;
    column_type: FactTableColumnType;
}
