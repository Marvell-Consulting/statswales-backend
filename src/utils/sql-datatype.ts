// DuckDB reports DOUBLE for floating-point columns; PostgreSQL requires DOUBLE PRECISION.
export const normalizeSqlDatatype = (datatype: string): string =>
  datatype === 'DOUBLE' ? 'DOUBLE PRECISION' : datatype;

const NUMERIC_DATATYPE =
  /^(U?(TINY|SMALL|BIG|HUGE)?INT(EGER)?|INT[248]|FLOAT[48]?|REAL|DOUBLE( PRECISION)?|DECIMAL|NUMERIC)(\s*\(.*\))?$/i;

// True for integer, decimal and floating-point datatypes from either DuckDB or PostgreSQL.
export const isNumericSqlDatatype = (datatype: string): boolean => NUMERIC_DATATYPE.test(datatype.trim());
