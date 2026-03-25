export enum OutputFormats {
  Csv = 'csv',
  Json = 'json',
  Excel = 'xlsx',
  Frontend = 'frontend',
  Html = 'html'
  // Parquet = 'parquet'
}

const DOWNLOAD_FORMATS: ReadonlySet<OutputFormats> = new Set([
  OutputFormats.Csv,
  OutputFormats.Excel,
  OutputFormats.Json
]);

export function isDownloadFormat(format: OutputFormats): boolean {
  return DOWNLOAD_FORMATS.has(format);
}
