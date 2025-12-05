import { MeasureRowDto } from '../measure-row-dto';
import { DimensionMetadataDTO } from '../dimension-metadata-dto';

export class Measure {
  id: string;
  fact_table_column: string;
  measure_table: MeasureRowDto[] | undefined;
  metadata?: DimensionMetadataDTO;
}
