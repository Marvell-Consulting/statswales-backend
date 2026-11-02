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
      { fact_table_column: 'DescCode', dimension_name: 'DescCode', language: 'en-gb' }
    ]);
    mockGetByDatasetId.mockResolvedValue(dimensionNamed(''));

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual([]);
  });

  it('accepts an approval time returned as a string by a raw query', async () => {
    await getChangesSinceApproval({ ...approvedRevision, approved_at: approvedAt.toISOString() as unknown as Date });

    expect(mockGetColumnsWithLookupsUploadedSince).toHaveBeenCalledWith('ds-1', approvedAt);
  });

  it('treats a revision as changed when the check itself fails', async () => {
    mockGetPublishedDimensionNames.mockRejectedValue(new Error('relation "filter_table" does not exist'));

    const changes = await getChangesSinceApproval(approvedRevision);

    expect(changes).toEqual(['could not check for changes since approval']);
  });
});
