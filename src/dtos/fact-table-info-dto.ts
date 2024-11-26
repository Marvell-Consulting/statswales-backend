import { FactTableInfo } from '../entities/dataset/fact-table-info';

export class FactTableInfoDto {
    fact_table_id: string;
    column_name: string;
    column_index: number;
    type?: string | undefined;

    static fromFactTableInfo(factTableInfo: FactTableInfo): FactTableInfoDto {
        const infoDto = new FactTableInfoDto();
        infoDto.fact_table_id = factTableInfo.id;
        infoDto.column_name = factTableInfo.columnName;
        infoDto.column_index = factTableInfo.columnIndex;
        infoDto.type = factTableInfo.columnType;
        return infoDto;
    }
}
