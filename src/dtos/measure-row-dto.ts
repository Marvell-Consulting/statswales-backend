import { MeasureRow } from '../entities/dataset/measure-row';

export class MeasureRowDto {
  measure_id: string;
  language: string;
  reference: string;
  sort_order?: number;
  description: string;
  notes?: string;
  format: string;
  decimals?: number;
  measure_type?: string;
  hierarchy?: string;

  static fromMeasureRow(row: MeasureRow) {
    const dto = new MeasureRowDto();
    dto.measure_id = row.id;
    dto.language = row.language;
    dto.reference = row.reference;
    dto.sort_order = row.sortOrder || undefined;
    dto.description = row.description;
    dto.notes = row.notes || undefined;
    dto.format = row.format;
    dto.decimals = row.decimal || undefined;
    dto.measure_type = row.measureType || undefined;
    dto.hierarchy = row.hierarchy || undefined;
    return dto;
  }
}
