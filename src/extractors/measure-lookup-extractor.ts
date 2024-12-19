import { ColumnDescriptor } from './column-descriptor';

export interface MeasureLookupTableExtractor {
    descriptionColumns: ColumnDescriptor[];
    sortColumn: string | undefined;
    notesColumns: ColumnDescriptor[] | undefined;
    measureTypeColumn: string;
    formatColumn: string;
}
