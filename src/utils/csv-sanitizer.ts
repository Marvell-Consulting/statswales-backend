// Neutralizes CSV/XLSX formula injection (CWE-1236). String values beginning with a character
// that Excel/LibreOffice/Sheets treats as a formula prefix are given a leading apostrophe so they
// are opened as inert text instead of being evaluated. Non-string values (numbers, booleans,
// dates, null/undefined) are left untouched — e.g. a genuine negative number must not be turned
// into the string "'-5" just because "-" is a dangerous leading character for strings.
export function neutralizeCsvCell(value: string): string;
export function neutralizeCsvCell<T>(value: T): T;
export function neutralizeCsvCell(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  return /^[=+\-@\t\r\n]/.test(value) ? `'${value}` : value;
}

// Neutralizes every value in a positional row/array — CSV/XLSX data rows or header arrays.
export function neutralizeCsvRow(row: string[]): string[];
export function neutralizeCsvRow<T>(row: T[]): T[];
export function neutralizeCsvRow(row: unknown[]): unknown[] {
  return row.map((value) => neutralizeCsvCell(value));
}

// Neutralizes every key and value of a plain record. Use this wherever the record's own keys
// become the header row (e.g. csv-stringify/fast-csv with header/headers: true).
export const neutralizeCsvRecord = (record: Record<string, unknown>): Record<string, unknown> => {
  const sanitized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) {
    sanitized[neutralizeCsvCell(key)] = neutralizeCsvCell(value);
  }
  return sanitized;
};
