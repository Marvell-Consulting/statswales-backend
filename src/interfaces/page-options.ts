import { Locale } from '../enums/locale';
import { OutputFormats } from '../enums/output-formats';

export interface PageOptions {
  format: OutputFormats;
  pageNumber: number;
  pageSize?: number;
  sort: string[];
  locale: Locale;
}
