// DuckDB reports DOUBLE for floating-point columns; PostgreSQL requires DOUBLE PRECISION.
export const normalizeSqlDatatype = (datatype: string): string =>
  datatype === 'DOUBLE' ? 'DOUBLE PRECISION' : datatype;
