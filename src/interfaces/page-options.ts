import { Locale } from '../enums/locale';
import { OutputFormats } from '../enums/output-formats';

export interface PageOptions {
  format: OutputFormats;
  pageNumber: number;
  pageSize?: number;
  sort: string[];
  locale: Locale;
  y?: string[] | string;
  x?: string[] | string;
  // Opaque keyset-pagination cursor. When supplied, the caller is opting in
  // to cursor-based pagination and page_number is ignored beyond its default
  // of 1. Mutually exclusive with non-default page_number.
  cursor?: string;
}
