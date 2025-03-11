import { ColumnDescriptor } from './column-descriptor';

export interface LookupTableExtractor {
  descriptionColumns: ColumnDescriptor[];
  sortColumn?: string;
  hierarchyColumn?: string;
  notesColumns?: ColumnDescriptor[];
  languageColumn?: string;
}
