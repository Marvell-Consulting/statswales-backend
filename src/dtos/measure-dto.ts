import { Measure } from '../entities/dataset/measure';

import { LookupTableDTO } from './lookup-table-dto';
import { MeasureRowDto } from './measure-row-dto';
import { DimensionMetadataDTO } from './dimension-metadata-dto';

export class MeasureDTO {
    id: string;
    dataset_id: string;
    fact_table_column: string;
    join_column: string | null;
    lookup_table?: LookupTableDTO;
    measure_table: MeasureRowDto[] | undefined;
    metadata?: DimensionMetadataDTO[];

    static fromMeasure(measure: Measure): MeasureDTO {
        const dto = new MeasureDTO();
        dto.id = measure.id;
        dto.measure_table = measure.measureTable?.map((info) => {
            return MeasureRowDto.fromMeasureRow(info);
        });
        dto.metadata = measure.metadata
            ? measure.metadata.map((metadata) => {
                  return DimensionMetadataDTO.fromDimensionMetadata(metadata);
              })
            : undefined;
        dto.join_column = measure.joinColumn;
        dto.fact_table_column = measure.factTableColumn;
        dto.lookup_table = measure.lookupTable ? LookupTableDTO.fromLookupTable(measure.lookupTable) : undefined;
        return dto;
    }
}
