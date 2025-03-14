import { ColumnDescriptor } from './column-descriptor';

export interface MeasureLookupTableExtractor {
  descriptionColumns: ColumnDescriptor[];
  sortColumn?: string;
  notesColumns?: ColumnDescriptor[];
  measureTypeColumn?: string;
  formatColumn?: string;
  decimalColumn?: string;
  languageColumn?: string;
  isSW2Format: boolean;
}
