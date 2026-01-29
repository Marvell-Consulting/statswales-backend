jest.mock('../../src/utils/view-error-generators', () => ({
  viewGenerator: jest.fn((_dataset, page, pageInfo, size, totalPages, headers, data) => ({
    dataset: { id: 'stub' },
    current_page: page,
    page_info: pageInfo,
    page_size: size,
    total_pages: totalPages,
    headers,
    data
  }))
}));

import { Dataset } from '../../src/entities/dataset/dataset';
import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { previewGenerator, sampleSize } from '../../src/utils/preview-generator';
import { viewGenerator } from '../../src/utils/view-error-generators';

describe('preview-generator', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const dataset = { id: 'ds-1' } as unknown as Dataset;

  describe('previewGenerator', () => {
    it('should create headers from preview object keys', () => {
      const preview = [{ col_a: 'val1', col_b: 'val2' }];
      previewGenerator(preview, { totalLines: 10 }, dataset, false);

      const headers = (viewGenerator as jest.Mock).mock.calls[0][5];
      expect(headers.map((h: { name: string }) => h.name)).toEqual(['col_a', 'col_b']);
    });

    it('should set Unknown source_type on all headers', () => {
      const preview = [{ col_a: 'val1' }];
      previewGenerator(preview, { totalLines: 10 }, dataset, false);

      const headers = (viewGenerator as jest.Mock).mock.calls[0][5];
      expect(headers[0].source_type).toBe(FactTableColumnType.Unknown);
    });

    it('should convert preview rows to arrays of values', () => {
      const preview = [
        { col_a: 'r1a', col_b: 'r1b' },
        { col_a: 'r2a', col_b: 'r2b' }
      ];
      previewGenerator(preview, { totalLines: 10 }, dataset, false);

      const data = (viewGenerator as jest.Mock).mock.calls[0][6];
      expect(data).toEqual([
        ['r1a', 'r1b'],
        ['r2a', 'r2b']
      ]);
    });

    it('should use preview.length as pageSize when sample is false', () => {
      const preview = [{ a: '1' }, { a: '2' }, { a: '3' }];
      previewGenerator(preview, { totalLines: 10 }, dataset, false);

      const size = (viewGenerator as jest.Mock).mock.calls[0][3];
      expect(size).toBe(3);
    });

    it('should cap pageSize at sampleSize when sample is true and preview is larger', () => {
      const preview = Array.from({ length: sampleSize + 3 }, (_, i) => ({ a: String(i) }));
      previewGenerator(preview, { totalLines: 100 }, dataset, true);

      const size = (viewGenerator as jest.Mock).mock.calls[0][3];
      expect(size).toBe(sampleSize);
    });

    it('should use preview.length as pageSize when sample is true but preview is smaller', () => {
      const preview = [{ a: '1' }, { a: '2' }];
      previewGenerator(preview, { totalLines: 10 }, dataset, true);

      const size = (viewGenerator as jest.Mock).mock.calls[0][3];
      expect(size).toBe(2);
    });

    it('should pass correct pageInfo with totals', () => {
      const preview = [{ a: '1' }, { a: '2' }];
      previewGenerator(preview, { totalLines: 42 }, dataset, false);

      const pageInfo = (viewGenerator as jest.Mock).mock.calls[0][2];
      expect(pageInfo).toEqual({
        total_records: 42,
        start_record: 1,
        end_record: 2
      });
    });
  });
});
