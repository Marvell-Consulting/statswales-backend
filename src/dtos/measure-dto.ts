import { Measure } from '../entities/dataset/measure';

import { LookupTableDTO } from './lookup-table-dto';
import { MeasureRowDto } from './measure-row-dto';

export class MeasureDTO {
    id: string;
    dataset_id: string;
    fact_table_column: string;
    join_column: string | null;
    lookup_table?: LookupTableDTO;
    measure_table: MeasureRowDto[] | undefined;

    static fromMeasure(measure: Measure): MeasureDTO {
        const dto = new MeasureDTO();
        dto.id = measure.id;
        dto.measure_table = measure.measureTable?.map((info) => {
            return MeasureRowDto.fromMeasureRow(info);
        });
        dto.join_column = measure.joinColumn;
        dto.fact_table_column = measure.factTableColumn;
        dto.lookup_table = measure.lookupTable ? LookupTableDTO.fromLookupTable(measure.lookupTable) : undefined;
        return dto;
    }
}
