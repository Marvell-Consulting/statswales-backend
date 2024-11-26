import {
    Entity,
    PrimaryColumn,
    Column,
    BaseEntity,
    ManyToOne,
    JoinColumn
} from 'typeorm';

import { FactTable } from './fact-table';
import { SourceType } from '../../enums/source-type';

@Entity({ name: 'fact_table_info' })
export class FactTableInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'fact_table_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_fact_table_info_id_language'
    })
    id: string;

    @PrimaryColumn({
        name: 'column_name',
        type: 'varchar',
        primaryKeyConstraintName: 'PK_fact_table_info_id_language'
    })
    columnName: string;

    @Column({ name: 'column_index', type: 'integer' })
    columnIndex: number;

    @Column({ name: 'column_type', type: 'enum', enum: Object.values(SourceType), nullable: false })
    columnType: SourceType;

    @ManyToOne(() => FactTable, (factTable) => factTable.factTableInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'fact_table_id', foreignKeyConstraintName: 'FK_fact_table_info_fact_table_id_id' })
    factTable: FactTable;
}
