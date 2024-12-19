import { ColumnDescriptor } from './column-descriptor';

export interface LookupTableExtractor {
    descriptionColumns: ColumnDescriptor[];
    sortColumn: string | undefined;
    hierarchyColumn: string | undefined;
    notesColumns: ColumnDescriptor[] | undefined;
}
