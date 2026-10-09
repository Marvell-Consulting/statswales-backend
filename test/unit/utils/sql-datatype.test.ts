import { isNumericSqlDatatype, normalizeSqlDatatype } from '../../../src/utils/sql-datatype';

describe('normalizeSqlDatatype', () => {
  it('maps DuckDB DOUBLE to PostgreSQL DOUBLE PRECISION', () => {
    expect(normalizeSqlDatatype('DOUBLE')).toBe('DOUBLE PRECISION');
  });

  it('leaves other datatypes unchanged', () => {
    expect(normalizeSqlDatatype('BIGINT')).toBe('BIGINT');
  });
});

describe('isNumericSqlDatatype', () => {
  it.each([
    'TINYINT',
    'SMALLINT',
    'INTEGER',
    'INT',
    'BIGINT',
    'HUGEINT',
    'UBIGINT',
    'INT8',
    'FLOAT',
    'REAL',
    'DOUBLE',
    'DOUBLE PRECISION',
    'DECIMAL(18,3)',
    'NUMERIC',
    'bigint'
  ])('treats %s as numeric', (datatype) => {
    expect(isNumericSqlDatatype(datatype)).toBe(true);
  });

  it.each(['VARCHAR', 'TEXT', 'TIME', 'DATE', 'TIMESTAMP', 'INTERVAL', 'BOOLEAN'])(
    'treats %s as non-numeric',
    (datatype) => {
      expect(isNumericSqlDatatype(datatype)).toBe(false);
    }
  );
});
