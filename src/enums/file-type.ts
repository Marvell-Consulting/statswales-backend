/*
    The Following types are all supported natively by DuckDB
    for import and querying and we should support them.
 */
export enum FileType {
    Csv = 'csv',
    Parquet = 'parquet',
    Json = 'json',
    Excel = 'xlsx',
    GzipCsv = 'csv.gz',
    GzipJson = 'json.gz',
    Unknown = 'unknown'
}
