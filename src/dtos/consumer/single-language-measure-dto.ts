import { MeasureRowDto } from '../measure-row-dto';
import { DimensionMetadataDTO } from '../dimension-metadata-dto';
import { Measure } from '../../entities/dataset/measure';
import { MeasureMetadata } from '../../entities/dataset/measure-metadata';
import { MeasureRow } from '../../entities/dataset/measure-row';

export class SingleLanguageMeasureDTO {
  id: string;
  fact_table_column: string;
  measure_table?: MeasureRowDto[];
  metadata?: DimensionMetadataDTO;

  static fromMeasure(measure: Measure, lang: string): SingleLanguageMeasureDTO {
    const dto = new SingleLanguageMeasureDTO();
    dto.id = measure.id;
    dto.fact_table_column = measure.factTableColumn;

    dto.measure_table = measure.measureTable
      ?.filter((row: MeasureRow) => row.language === lang)
      .map((row: MeasureRow) => MeasureRowDto.fromMeasureRow(row));

    const metadata = measure.metadata?.find((meta: MeasureMetadata) => meta.language === lang);
    if (metadata) {
      dto.metadata = DimensionMetadataDTO.fromDimensionMetadata(metadata);
    }

    return dto;
  }
}
