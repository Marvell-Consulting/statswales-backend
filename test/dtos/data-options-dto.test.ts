import 'reflect-metadata';
import { plainToInstance } from 'class-transformer';
import { validateSync, ValidationError } from 'class-validator';
import { DataOptionsDTO } from '../../src/dtos/data-options-dto';

const findErrorByProperty = (errors: ValidationError[], property: string): ValidationError | undefined => {
  for (const error of errors) {
    if (error.property === property) return error;
    const child = findErrorByProperty(error.children ?? [], property);
    if (child) return child;
  }
  return undefined;
};

describe('DataOptionsDTO validation', () => {
  it('accepts a valid payload', () => {
    const dto = plainToInstance(DataOptionsDTO, {
      pivot: {
        backend: 'duckdb',
        include_performance: false,
        x: ['dim1'],
        y: 'measure1'
      },
      filters: [{ region: ['wales'] }],
      options: {
        use_raw_column_names: true,
        use_reference_values: false,
        data_value_type: 'formatted'
      }
    });

    const errors = validateSync(dto, { whitelist: true });
    expect(errors).toHaveLength(0);
  });

  it('rejects invalid pivot backend', () => {
    const dto = plainToInstance(DataOptionsDTO, {
      pivot: {
        backend: 'mysql',
        include_performance: false,
        x: 'dim1',
        y: 'measure1'
      },
      filters: [{ region: ['wales'] }]
    });

    const errors = validateSync(dto, { whitelist: true });
    const backendError = findErrorByProperty(errors, 'backend');
    expect(backendError?.constraints?.isEnum).toBeDefined();
  });

  it('rejects invalid options data_value_type', () => {
    const dto = plainToInstance(DataOptionsDTO, {
      pivot: {
        backend: 'duckdb',
        include_performance: false,
        x: 'dim1',
        y: 'measure1'
      },
      filters: [{ region: ['wales'] }],
      options: {
        data_value_type: 'invalid-value'
      }
    });

    const errors = validateSync(dto, { whitelist: true });
    const typeError = findErrorByProperty(errors, 'data_value_type');
    expect(typeError?.constraints?.isEnum).toBeDefined();
  });

  it('rejects non-array filters', () => {
    const dto = plainToInstance(DataOptionsDTO, {
      filters: { region: ['wales'] }
    });

    const errors = validateSync(dto, { whitelist: true });
    const filtersError = findErrorByProperty(errors, 'filters');
    expect(filtersError?.constraints?.isArray).toBeDefined();
  });
});
