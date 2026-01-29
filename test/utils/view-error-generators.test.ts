jest.mock('i18next', () => ({
  t: jest.fn((tag: string, params: Record<string, unknown>) => `translated:${tag}:${params?.lng}`)
}));

jest.mock('../../src/middleware/translation', () => ({
  AVAILABLE_LANGUAGES: ['en', 'cy']
}));

jest.mock('../../src/dtos/dataset-dto', () => ({
  DatasetDTO: {
    fromDataset: jest.fn().mockReturnValue({ id: 'dataset-stub' })
  }
}));

jest.mock('../../src/dtos/data-table-dto', () => ({
  DataTableDto: {
    fromDataTable: jest.fn().mockReturnValue({ id: 'datatable-stub' })
  }
}));

import { Dataset } from '../../src/entities/dataset/dataset';
import { DataTable } from '../../src/entities/dataset/data-table';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { DataTableDto } from '../../src/dtos/data-table-dto';
import { viewErrorGenerators, viewGenerator } from '../../src/utils/view-error-generators';

describe('view-error-generators', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('viewErrorGenerators', () => {
    it('should return a ViewErrDTO with status, dataset_id and field', () => {
      const result = viewErrorGenerators(400, 'ds-1', 'name', 'errors.bad_field', {});

      expect(result.status).toBe(400);
      expect(result.dataset_id).toBe('ds-1');
      expect(result.errors[0].field).toBe('name');
    });

    it('should generate multilingual user messages for each available language', () => {
      const result = viewErrorGenerators(400, 'ds-1', 'name', 'errors.bad_field', {});

      expect(result.errors[0].user_message).toHaveLength(2);
      expect(result.errors[0].user_message[0]).toEqual({
        message: 'translated:errors.bad_field:en',
        lang: 'en'
      });
      expect(result.errors[0].user_message[1]).toEqual({
        message: 'translated:errors.bad_field:cy',
        lang: 'cy'
      });
    });

    it('should include the message key and params', () => {
      const result = viewErrorGenerators(400, 'ds-1', 'name', 'errors.bad_field', {}, { count: 5 });

      expect(result.errors[0].message).toEqual({
        key: 'errors.bad_field',
        params: { count: 5 }
      });
    });

    it('should pass params to i18next t function', () => {
      const { t } = jest.requireMock('i18next');
      viewErrorGenerators(400, 'ds-1', 'name', 'errors.test', {}, { limit: 10 });

      expect(t).toHaveBeenCalledWith('errors.test', expect.objectContaining({ limit: 10, lng: 'en' }));
      expect(t).toHaveBeenCalledWith('errors.test', expect.objectContaining({ limit: 10, lng: 'cy' }));
    });

    it('should include the extension object', () => {
      const extension = { extra: 'data' };
      const result = viewErrorGenerators(400, 'ds-1', 'name', 'errors.test', extension);

      expect(result.extension).toEqual({ extra: 'data' });
    });
  });

  describe('viewGenerator', () => {
    it('should call DatasetDTO.fromDataset and map fields', () => {
      const dataset = { id: 'ds-1' } as unknown as Dataset;
      const headers = [{ index: 0, name: 'col_a' }];
      const data = [['val1']];
      const pageInfo = { total_records: 10, start_record: 1, end_record: 5 };

      const result = viewGenerator(dataset, 1, pageInfo, 5, 2, headers, data);

      expect(DatasetDTO.fromDataset).toHaveBeenCalledWith(dataset);
      expect(result.dataset).toEqual({ id: 'dataset-stub' });
      expect(result.current_page).toBe(1);
      expect(result.page_size).toBe(5);
      expect(result.total_pages).toBe(2);
      expect(result.headers).toBe(headers);
      expect(result.data).toBe(data);
      expect(result.page_info).toBe(pageInfo);
    });

    it('should map dataTable when provided', () => {
      const dataset = { id: 'ds-1' } as unknown as Dataset;
      const dataTable = { id: 'dt-1' } as unknown as DataTable;

      const result = viewGenerator(dataset, 1, {}, 10, 1, [], [], dataTable);

      expect(DataTableDto.fromDataTable).toHaveBeenCalledWith(dataTable);
      expect(result.data_table).toEqual({ id: 'datatable-stub' });
    });

    it('should leave data_table undefined when not provided', () => {
      const dataset = { id: 'ds-1' } as unknown as Dataset;

      const result = viewGenerator(dataset, 1, {}, 10, 1, [], []);

      expect(result.data_table).toBeUndefined();
    });
  });
});
