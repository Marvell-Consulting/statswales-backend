jest.mock('../../../src/utils/logger', () => ({
  logger: { warn: jest.fn(), info: jest.fn(), debug: jest.fn(), error: jest.fn() }
}));

const mockGetColumnsWithLookupsUploadedSince = jest.fn();
const mockGetByDatasetId = jest.fn();
jest.mock('../../../src/repositories/dimension', () => ({
  DimensionRepository: {
    getColumnsWithLookupsUploadedSince: (...args: unknown[]) => mockGetColumnsWithLookupsUploadedSince(...args),
    getByDatasetId: (...args: unknown[]) => mockGetByDatasetId(...args)
  }
}));

const mockGetPublishedDimensionNames = jest.fn();
jest.mock('../../../src/repositories/published-dataset', () => ({
  PublishedDatasetRepository: {
    getPublishedDimensionNames: (...args: unknown[]) => mockGetPublishedDimensionNames(...args)
  }
}));

import { getChangesSinceApproval } from '../../../src/services/changes-since-approval';
import { RevisionList } from '../../../src/repositories/revision';

describe('getChangesSinceApproval', () => {
  const approvedAt = new Date('2026-04-13T11:01:50Z');

  const approvedRevision: RevisionList = {
    id: 'rev-1',
    dataset_id: 'ds-1',
    data_table_id: 'dt-1',
    revision_index: 1,
    approved_at: approvedAt
  };

  const cubeNames = [
    { fact_table_column: 'DescCode', dimension_name: 'Row description', language: 'en-gb' },
    { fact_table_column: 'DescCode', dimension_name: 'Disgrifiad rhes', language: 'cy-gb' }
  ];

  const dimensionNamed = (en: string, cy = 'Disgrifiad rhes') => [
    {
      factTableColumn: 'DescCode',
      metadata: [
        { language: 'en-GB', name: en },
        { language: 'cy-GB', name: cy }
      ]
    }
  ];

  beforeEach(() => {
    jest.clearAllMocks();
    mockGetColumnsWithLookupsUploadedSince.mockResolvedValue([]);
    mockGetPublishedDimensionNames.mockResolvedValue(cubeNames);
    mockGetByDatasetId.mockResolvedValue(dimensionNamed('Row description'));
  });

  it('returns no changes for a draft revision without querying anything', async () => {
    const changes = await getChangesSinceApproval({ ...approvedRevision, approved_at: null });

    expect(changes).toEqual([]);
    expect(mockGetColumnsWithLookupsUploadedSince).not.toHaveBeenCalled();
    expect(mockGetPublishedDimensionNames).not.toHaveBeenCalled();
  });

  it('returns no changes when names and lookups match the approved revision', async () => {
    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual([]);
    expect(mockGetColumnsWithLookupsUploadedSince).toHaveBeenCalledWith('ds-1', approvedAt);
    expect(mockGetPublishedDimensionNames).toHaveBeenCalledWith('rev-1');
  });

  it('reports a dimension renamed after approval', async () => {
    mockGetByDatasetId.mockResolvedValue(dimensionNamed('Amount'));

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual(['DescCode (en-GB) renamed after approval']);
  });

  it('reports lookup tables replaced after approval', async () => {
    mockGetColumnsWithLookupsUploadedSince.mockResolvedValue(['DescCode', 'Year']);

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual([
      'lookup table for DescCode replaced after approval',
      'lookup table for Year replaced after approval'
    ]);
  });

  it('compares unnamed dimensions using the fact table column, as the cube builder does', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([
      { fact_table_column: 'DescCode', dimension_name: 'DescCode', language: 'en-gb' },
      { fact_table_column: 'DescCode', dimension_name: 'Disgrifiad rhes', language: 'cy-gb' }
    ]);
    mockGetByDatasetId.mockResolvedValue(dimensionNamed(''));

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual([]);
  });

  it('accepts an approval time returned as a string by a raw query', async () => {
    await getChangesSinceApproval({ ...approvedRevision, approved_at: approvedAt.toISOString() as unknown as Date });

    expect(mockGetColumnsWithLookupsUploadedSince).toHaveBeenCalledWith('ds-1', approvedAt);
  });

  it('accepts the numbered suffix the cube builder adds when two dimensions share a name', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([
      { fact_table_column: 'DescCode', dimension_name: 'Description', language: 'en-gb' },
      { fact_table_column: 'OtherCode', dimension_name: 'Description_1', language: 'en-gb' }
    ]);
    mockGetByDatasetId.mockResolvedValue([
      { factTableColumn: 'DescCode', metadata: [{ language: 'en-GB', name: 'Description' }] },
      { factTableColumn: 'OtherCode', metadata: [{ language: 'en-GB', name: 'Description' }] }
    ]);

    expect(await getChangesSinceApproval(approvedRevision)).toEqual([]);
  });

  it('reports a rename that only removes a numbered suffix when the name is not duplicated', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([
      { fact_table_column: 'DescCode', dimension_name: 'Row description_1', language: 'en-gb' },
      { fact_table_column: 'DescCode', dimension_name: 'Disgrifiad rhes', language: 'cy-gb' }
    ]);

    expect(await getChangesSinceApproval(approvedRevision)).toEqual(['DescCode (en-GB) renamed after approval']);
  });

  it('does not treat a longer name as a numbered suffix', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([
      { fact_table_column: 'DescCode', dimension_name: 'Row description_old', language: 'en-gb' },
      { fact_table_column: 'DescCode', dimension_name: 'Disgrifiad rhes', language: 'cy-gb' }
    ]);

    expect(await getChangesSinceApproval(approvedRevision)).toEqual(['DescCode (en-GB) renamed after approval']);
  });

  it('reports a language that has no name in the cube', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([
      { fact_table_column: 'DescCode', dimension_name: 'Row description', language: 'en-gb' }
    ]);

    expect(await getChangesSinceApproval(approvedRevision)).toEqual(['DescCode (cy-GB) has no name in the cube']);
  });

  it('reports a dimension that is not in the cube', async () => {
    mockGetPublishedDimensionNames.mockResolvedValue([]);

    expect(await getChangesSinceApproval(approvedRevision)).toEqual(['DescCode is not in the cube']);
  });

  it('reports an approval time that is not a valid date without querying anything', async () => {
    const changes = await getChangesSinceApproval({
      ...approvedRevision,
      approved_at: 'not a date' as unknown as Date
    });

    expect(changes).toEqual(['approval time "not a date" is not a valid date']);
    expect(mockGetColumnsWithLookupsUploadedSince).not.toHaveBeenCalled();
  });

  it('gives a specific reason when the cube has no filter table', async () => {
    mockGetPublishedDimensionNames.mockRejectedValue(
      Object.assign(new Error('relation "rev-1.filter_table" does not exist'), { code: '42P01' })
    );

    expect(await getChangesSinceApproval(approvedRevision)).toEqual([
      'cube has no filter table, so names cannot be checked'
    ]);
  });

  it('only reports a missing filter table when the cube query is the one that failed', async () => {
    mockGetColumnsWithLookupsUploadedSince.mockRejectedValue(
      Object.assign(new Error('relation "lookup_table" does not exist'), { code: '42P01' })
    );

    expect(await getChangesSinceApproval(approvedRevision)).toEqual(['could not check for changes since approval']);
  });

  it('treats a revision as changed when the check itself fails, keeping reasons already found', async () => {
    mockGetColumnsWithLookupsUploadedSince.mockResolvedValue(['Year']);
    mockGetPublishedDimensionNames.mockRejectedValue(new Error('connection terminated'));

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual([
      'lookup table for Year replaced after approval',
      'could not check for changes since approval'
    ]);
  });
});
