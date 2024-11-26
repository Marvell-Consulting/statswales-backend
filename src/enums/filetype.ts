/*
    The Following types are all supported natively by DuckDB
    for import and querying and we should support them.
 */
export enum Filetype {
    Csv = 'csv',
    Parquet = 'parquet',
    Json = 'json',
    Excel = 'xlsx',
    Unknown = 'unknown'
}
