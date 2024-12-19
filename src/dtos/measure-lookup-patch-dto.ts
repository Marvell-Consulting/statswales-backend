import { ColumnDescriptor } from '../extractors/column-descriptor';

export interface MeasureLookupPatchDTO {
    join_column: string;
    description_columns: string[];
    notes_columns: string[] | undefined;
    sort_column: string | undefined;
    measure_type_column: string;
    format_column: string;
}
