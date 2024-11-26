import { Measure } from '../entities/dataset/measure';

import { LookupTableDTO } from './lookup-table-dto';

export class MeasureDTO {
    id: string;
    dataset_id: string;
    fact_table_column: string;
    join_column: string;
    lookup_table?: LookupTableDTO;

    static fromMeasure(measure: Measure): MeasureDTO {
        const dto = new MeasureDTO();
        dto.id = measure.id;
        dto.dataset_id = measure.dataset.id;
        dto.join_column = measure.joinColumn;
        dto.fact_table_column = measure.factTableColumn;
        dto.lookup_table = LookupTableDTO.fromLookupTable(measure.lookupTable) || undefined;
        return dto;
    }
}
