import { Request } from 'express';
import { parsePageOptions } from '../../src/utils/parse-page-options';
import { OutputFormats } from '../../src/enums/output-formats';
import { Locale } from '../../src/enums/locale';
import { BadRequestException } from '../../src/exceptions/bad-request.exception';
import { DEFAULT_PAGE_SIZE } from '../../src/utils/page-defaults';

// Mock the validators
jest.mock('../../src/validators', () => ({
  format2Validator: jest.fn(),
  pageNumberValidator: jest.fn(),
  pageSizeValidator: jest.fn()
}));

// Mock express-validator
jest.mock('express-validator', () => ({
  matchedData: jest.fn(),
  FieldValidationError: jest.fn()
}));

// Mock logger
jest.mock('../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    error: jest.fn(),
    warn: jest.fn()
  }
}));

import { format2Validator, pageNumberValidator, pageSizeValidator } from '../../src/validators';
import { matchedData } from 'express-validator';

type MockRequest = Partial<Request> & { language?: Locale };

describe('parsePageOptions', () => {
  let mockRequest: MockRequest;
  let mockValidationResult: any;

  beforeEach(() => {
    jest.clearAllMocks();

    mockRequest = {
      query: {},
      language: Locale.English
    } as Partial<Request>;

    mockValidationResult = {
      isEmpty: jest.fn().mockReturnValue(true),
      array: jest.fn().mockReturnValue([])
    };

    // Default mock implementations for validators
    (format2Validator as jest.Mock).mockReturnValue({
      run: jest.fn().mockResolvedValue(mockValidationResult)
    });
    (pageNumberValidator as jest.Mock).mockReturnValue({
      run: jest.fn().mockResolvedValue(mockValidationResult)
    });
    (pageSizeValidator as jest.Mock).mockReturnValue({
      run: jest.fn().mockResolvedValue(mockValidationResult)
    });

    // Default matchedData returns empty object
    (matchedData as jest.Mock).mockReturnValue({});
  });

  describe('successful parsing', () => {
    it('should return default values when no query parameters are provided', async () => {
      const result = await parsePageOptions(mockRequest as Request);

      expect(result).toEqual({
        format: OutputFormats.Json,
        pageNumber: 1,
        pageSize: DEFAULT_PAGE_SIZE,
        sort: [],
        locale: Locale.English
      });
    });

    it('should parse all valid query parameters', async () => {
      mockRequest.query = {
        format: 'csv',
        page_number: '2',
        page_size: '50',
        sort_by: JSON.stringify([{ columnName: 'name', direction: 'ASC' }])
      };

      (matchedData as jest.Mock).mockReturnValue({
        format: OutputFormats.Csv,
        page_number: 2,
        page_size: 50
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result).toEqual({
        format: OutputFormats.Csv,
        pageNumber: 2,
        pageSize: 50,
        sort: ['name|asc'],
        locale: Locale.English
      });
    });

    it('should parse format as json when specified with default pageSize', async () => {
      mockRequest.query = {
        format: 'json'
      };

      (matchedData as jest.Mock).mockReturnValue({
        format: OutputFormats.Json
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.format).toBe(OutputFormats.Json);
      expect(result.pageSize).toBe(DEFAULT_PAGE_SIZE);
    });

    it('should parse format as csv when specified with undefined pageSize', async () => {
      mockRequest.query = {
        format: 'csv'
      };

      (matchedData as jest.Mock).mockReturnValue({
        format: OutputFormats.Csv
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.format).toBe(OutputFormats.Csv);
      expect(result.pageSize).toBeUndefined();
    });

    it('should parse format as xlsx when specified with undefined pageSize', async () => {
      mockRequest.query = {
        format: 'xlsx'
      };

      (matchedData as jest.Mock).mockReturnValue({
        format: OutputFormats.Excel
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.format).toBe(OutputFormats.Excel);
      expect(result.pageSize).toBeUndefined();
    });

    it('should parse format as frontend when specified with default pageSize', async () => {
      mockRequest.query = {
        format: 'frontend'
      };

      (matchedData as jest.Mock).mockReturnValue({
        format: OutputFormats.Frontend
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.format).toBe(OutputFormats.Frontend);
      expect(result.pageSize).toBe(DEFAULT_PAGE_SIZE);
    });

    it('should parse page number correctly', async () => {
      mockRequest.query = {
        page_number: '5'
      };

      (matchedData as jest.Mock).mockReturnValue({
        page_number: 5
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.pageNumber).toBe(5);
    });

    it('should parse page size correctly', async () => {
      mockRequest.query = {
        page_size: '25'
      };

      (matchedData as jest.Mock).mockReturnValue({
        page_size: 25
      });

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.pageSize).toBe(25);
    });

    it('should parse Welsh locale correctly', async () => {
      mockRequest.language = Locale.Welsh;

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.locale).toBe(Locale.Welsh);
    });

    it('should parse EnglishGb locale correctly', async () => {
      mockRequest.language = Locale.EnglishGb;

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.locale).toBe(Locale.EnglishGb);
    });

    it('should parse WelshGb locale correctly', async () => {
      mockRequest.language = Locale.WelshGb;

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.locale).toBe(Locale.WelshGb);
    });
  });

  describe('sort_by parameter', () => {
    it('should parse single sort_by parameter with ASC direction', async () => {
      mockRequest.query = {
        sort_by: JSON.stringify([{ columnName: 'name', direction: 'ASC' }])
      };

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.sort).toEqual(['name|asc']);
    });

    it('should parse single sort_by parameter with DESC direction', async () => {
      mockRequest.query = {
        sort_by: JSON.stringify([{ columnName: 'age', direction: 'DESC' }])
      };

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.sort).toEqual(['age|desc']);
    });

    it('should parse sort_by parameter without direction (defaults to asc)', async () => {
      mockRequest.query = {
        sort_by: JSON.stringify([{ columnName: 'name' }])
      };

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.sort).toEqual(['name|asc']);
    });

    it('should parse multiple sort_by parameters', async () => {
      mockRequest.query = {
        sort_by: JSON.stringify([
          { columnName: 'name', direction: 'ASC' },
          { columnName: 'age', direction: 'DESC' },
          { columnName: 'city' }
        ])
      };

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.sort).toEqual(['name|asc', 'age|desc', 'city|asc']);
    });

    it('should return empty array when sort_by is not provided', async () => {
      mockRequest.query = {};

      const result = await parsePageOptions(mockRequest as Request);

      expect(result.sort).toEqual([]);
    });

    it('should throw BadRequestException when sort_by is invalid JSON', async () => {
      mockRequest.query = {
        sort_by: 'invalid-json'
      };

      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow(BadRequestException);
      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow('errors.invalid_sort_by');
    });

    it('should throw BadRequestException when sort_by is malformed', async () => {
      mockRequest.query = {
        sort_by: '{"columnName": "test"'
      };

      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow(BadRequestException);
      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow('errors.invalid_sort_by');
    });
  });

  describe('validation errors', () => {
    it('should throw BadRequestException when format validation fails', async () => {
      const errorResult = {
        isEmpty: jest.fn().mockReturnValue(false),
        array: jest.fn().mockReturnValue([
          {
            msg: 'Invalid format',
            path: 'format',
            type: 'field'
          }
        ])
      };

      (format2Validator as jest.Mock).mockReturnValue({
        run: jest.fn().mockResolvedValue(errorResult)
      });

      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow(BadRequestException);
      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow('Invalid format for format');
    });

    it('should throw BadRequestException when page_number validation fails', async () => {
      const errorResult = {
        isEmpty: jest.fn().mockReturnValue(false),
        array: jest.fn().mockReturnValue([
          {
            msg: 'Invalid page number',
            path: 'page_number',
            type: 'field'
          }
        ])
      };

      (pageNumberValidator as jest.Mock).mockReturnValue({
        run: jest.fn().mockResolvedValue(errorResult)
      });

      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow(BadRequestException);
      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow('Invalid page number for page_number');
    });

    it('should throw BadRequestException when page_size validation fails', async () => {
      const errorResult = {
        isEmpty: jest.fn().mockReturnValue(false),
        array: jest.fn().mockReturnValue([
          {
            msg: 'Invalid page size',
            path: 'page_size',
            type: 'field'
          }
        ])
      };

      (pageSizeValidator as jest.Mock).mockReturnValue({
        run: jest.fn().mockResolvedValue(errorResult)
      });

      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow(BadRequestException);
      await expect(parsePageOptions(mockRequest as Request)).rejects.toThrow('Invalid page size for page_size');
    });
  });

  describe('validator invocation', () => {
    it('should call all validators', async () => {
      const format2Mock = { run: jest.fn().mockResolvedValue(mockValidationResult) };
      const pageNumberMock = { run: jest.fn().mockResolvedValue(mockValidationResult) };
      const pageSizeMock = { run: jest.fn().mockResolvedValue(mockValidationResult) };

      (format2Validator as jest.Mock).mockReturnValue(format2Mock);
      (pageNumberValidator as jest.Mock).mockReturnValue(pageNumberMock);
      (pageSizeValidator as jest.Mock).mockReturnValue(pageSizeMock);

      await parsePageOptions(mockRequest as Request);

      expect(format2Validator).toHaveBeenCalled();
      expect(pageNumberValidator).toHaveBeenCalled();
      expect(pageSizeValidator).toHaveBeenCalled();
      expect(format2Mock.run).toHaveBeenCalledWith(mockRequest);
      expect(pageNumberMock.run).toHaveBeenCalledWith(mockRequest);
      expect(pageSizeMock.run).toHaveBeenCalledWith(mockRequest);
    });
  });
});
