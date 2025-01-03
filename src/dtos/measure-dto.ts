import { Measure } from '../entities/dataset/measure';

import { LookupTableDTO } from './lookup-table-dto';
import { MeasureInfoDTO } from './measure-info-dto';

export class MeasureDTO {
    id: string;
    dataset_id: string;
    fact_table_column: string;
    join_column: string | null;
    lookup_table?: LookupTableDTO;
    measure_info: MeasureInfoDTO[] | undefined;

    static fromMeasure(measure: Measure): MeasureDTO {
        const dto = new MeasureDTO();
        dto.id = measure.id;
        dto.measure_info = measure.measureInfo?.map((info) => {
            return MeasureInfoDTO.fromMeasureInfo(info);
        });
        dto.join_column = measure.joinColumn;
        dto.fact_table_column = measure.factTableColumn;
        dto.lookup_table = measure.lookupTable ? LookupTableDTO.fromLookupTable(measure.lookupTable) : undefined;
        return dto;
    }
}
