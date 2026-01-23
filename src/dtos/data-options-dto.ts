import { IsBoolean, IsEnum, IsOptional, IsString, IsArray, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';
import { DataValueType } from '../enums/data-value-type';
import { FilterV2 } from '../interfaces/filterInterface';

enum PivotBackend {
  Postgres = 'postgres',
  DuckDb = 'duckdb'
}

class PivotDTO {
  @IsEnum(PivotBackend)
  backend: PivotBackend; // Default: 'duckdb'

  @IsBoolean()
  include_performance: boolean; // Default: false

  @IsString({ each: true })
  x: string | string[];

  @IsString({ each: true })
  y: string | string[];
}

class ColumnOptionsDTO {
  @IsOptional()
  @IsBoolean()
  use_raw_column_names?: boolean;

  @IsOptional()
  @IsBoolean()
  use_reference_values?: boolean;

  @IsOptional()
  @IsEnum(DataValueType)
  data_value_type?: DataValueType;
}

export class DataOptionsDTO {
  @IsOptional()
  @ValidateNested()
  @Type(() => PivotDTO)
  pivot?: PivotDTO;

  @IsOptional()
  @IsArray()
  filters?: FilterV2[];

  @IsOptional()
  @ValidateNested()
  @Type(() => ColumnOptionsDTO)
  options?: ColumnOptionsDTO;
}

export class PivotOptionsDTO extends DataOptionsDTO {
  @ValidateNested()
  @Type(() => PivotDTO)
  declare pivot: PivotDTO;
}

export const DEFAULT_DATA_OPTIONS: DataOptionsDTO = {
  filters: [],
  options: {
    use_raw_column_names: true,
    use_reference_values: true,
    data_value_type: DataValueType.Raw
  }
};

export const FRONTEND_DATA_OPTIONS: DataOptionsDTO = {
  filters: [],
  options: {
    use_raw_column_names: false,
    use_reference_values: true,
    data_value_type: DataValueType.WithNoteCodes
  }
};
