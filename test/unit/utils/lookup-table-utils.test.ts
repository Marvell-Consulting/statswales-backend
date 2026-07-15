jest.mock('i18next', () => ({
  t: jest.fn((key: string) => {
    const parts = key.split('.');
    return parts[parts.length - 1] ?? key;
  })
}));

jest.mock('../../../src/middleware/translation', () => ({
  SUPPORTED_LOCALES: ['en-GB', 'cy-GB']
}));

import { lookForJoinColumn } from '../../../src/utils/lookup-table-utils';
import { DataTable } from '../../../src/entities/dataset/data-table';
import { DataTableDescription } from '../../../src/entities/dataset/data-table-description';
import { FileType } from '../../../src/enums/file-type';
import { Locale } from '../../../src/enums/locale';
import { MeasureLookupPatchDTO } from '../../../src/dtos/measure-lookup-patch-dto';

function makeDataTableDescription(columnName: string, idx = 0): DataTableDescription {
  return {
    id: 'dt-1',
    columnName,
    columnIndex: idx,
    columnDatatype: 'VARCHAR',
    factTableColumn: null
  } as unknown as DataTableDescription;
}

function makeProtoLookupTable(descriptionColumnNames: string[]): DataTable {
  return {
    id: 'dt-1',
    filename: 'lookup.csv',
    originalFilename: 'lookup.csv',
    mimeType: 'text/csv',
    fileType: FileType.Csv,
    encoding: 'utf-8',
    hash: 'abc123',
    dataTableDescriptions: descriptionColumnNames.map((name, i) => makeDataTableDescription(name, i)),
    action: 'add',
    sourceLocation: 'datalake'
  } as unknown as DataTable;
}

describe('lookForJoinColumn', () => {
  const protoLookupTable = makeProtoLookupTable(['ref_code', 'description_en']);

  it('returns the matcher-supplied join_column when it matches an uploaded header', () => {
    const tableMatcher: MeasureLookupPatchDTO = { join_column: 'ref_code' };

    const result = lookForJoinColumn(protoLookupTable, 'measure_col', Locale.English, tableMatcher);

    expect(result).toBe('ref_code');
  });

  it('throws when the matcher-supplied join_column does not match any uploaded header', () => {
    const tableMatcher: MeasureLookupPatchDTO = { join_column: 'x" OR 1=1 --' };

    expect(() => lookForJoinColumn(protoLookupTable, 'measure_col', Locale.English, tableMatcher)).toThrow();
  });

  it('falls back to a column starting with "ref" when no matcher is supplied', () => {
    const result = lookForJoinColumn(protoLookupTable, 'measure_col', Locale.English);

    expect(result).toBe('ref_code');
  });
});
