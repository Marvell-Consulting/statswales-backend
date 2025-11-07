import { Locale } from '../enums/locale';

import { ColumnDescriptor } from './column-descriptor';

export interface LookupTableExtractor {
  tableLanguage: Locale;
  joinColumn?: string;
  descriptionColumns: ColumnDescriptor[];
  sortColumn?: string;
  hierarchyColumn?: string;
  notesColumns?: ColumnDescriptor[];
  languageColumn?: string;
  otherColumns?: string[];
  isSW2Format: boolean;
}
