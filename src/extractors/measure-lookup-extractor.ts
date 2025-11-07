import { ColumnDescriptor } from './column-descriptor';
import { Locale } from '../enums/locale';

export interface MeasureLookupTableExtractor {
  tableLanguage: Locale;
  descriptionColumns: ColumnDescriptor[];
  sortColumn?: string;
  notesColumns?: ColumnDescriptor[];
  measureTypeColumn?: string;
  formatColumn?: string;
  decimalColumn?: string;
  languageColumn?: string;
  hierarchyColumn?: string;
  isSW2Format: boolean;
  otherColumns?: string[];
  joinColumn?: string;
}
