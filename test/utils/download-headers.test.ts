import { DownloadFormat } from '../../src/enums/download-format';
import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { getDownloadHeaders } from '../../src/utils/download-headers';

describe('download-headers', () => {
  describe('getDownloadHeaders', () => {
    it('should return JSON headers with correct content-type', () => {
      const headers = getDownloadHeaders('ds-1', DownloadFormat.Json, 1024);

      expect(headers['content-type']).toBe('application/json; charset=utf-8');
      expect(headers['content-disposition']).toBe('attachment;filename=ds-1.json');
      expect(headers['content-length']).toBe(1024);
    });

    it('should return CSV headers with correct content-type', () => {
      const headers = getDownloadHeaders('ds-1', DownloadFormat.Csv, 2048);

      expect(headers['content-type']).toBe('text/csv; charset=utf-8');
      expect(headers['content-disposition']).toBe('attachment;filename=ds-1.csv');
      expect(headers['content-length']).toBe(2048);
    });

    it('should return XLSX headers with correct content-type', () => {
      const headers = getDownloadHeaders('ds-1', DownloadFormat.Xlsx, 4096);

      expect(headers['content-type']).toBe('application/vnd.ms-excel');
      expect(headers['content-disposition']).toBe('attachment;filename=ds-1.xlsx');
      expect(headers['content-length']).toBe(4096);
    });

    it('should include the dataset ID in the filename', () => {
      const headers = getDownloadHeaders('my-dataset-id', DownloadFormat.Json, 100);

      expect(headers['content-disposition']).toBe('attachment;filename=my-dataset-id.json');
    });

    it('should set content-length correctly', () => {
      const headers = getDownloadHeaders('ds-1', DownloadFormat.Json, 999);

      expect(headers['content-length']).toBe(999);
    });

    it('should throw BadRequestException for unsupported format', () => {
      expect(() => getDownloadHeaders('ds-1', 'parquet', 1024)).toThrow(BadRequestException);
      expect(() => getDownloadHeaders('ds-1', 'parquet', 1024)).toThrow('unsupported file format');
    });
  });
});
