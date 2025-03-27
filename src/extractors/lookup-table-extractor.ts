import { Locale } from '../enums/locale';

import { ColumnDescriptor } from './column-descriptor';

export interface LookupTableExtractor {
  tableLanguage: Locale;
  descriptionColumns: ColumnDescriptor[];
  sortColumn?: string;
  hierarchyColumn?: string;
  notesColumns?: ColumnDescriptor[];
  languageColumn?: string;
  isSW2Format: boolean;
}
