import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { DataTable } from './data-table';

@Entity({ name: 'data_table_description' })
export class DataTableDescription extends BaseEntity {
  @PrimaryColumn({
    name: 'fact_table_id',
    type: 'uuid',
    primaryKeyConstraintName: 'PK_data_table_description_id_column_name'
  })
  id: string;

  @PrimaryColumn({
    name: 'column_name',
    type: 'varchar',
    primaryKeyConstraintName: 'PK_data_table_description_id_column_name'
  })
  columnName: string;

  @Column({ name: 'column_index', type: 'integer' })
  columnIndex: number;

  @Column({ name: 'column_datatype', type: 'varchar', nullable: false })
  columnDatatype: string;

  // This says how this should join against the Fact Table.  Ideally this should be the same
  // as the columnName.
  @Column({ name: 'fact_table_column', type: 'text', nullable: true })
  factTableColumn: string;

  @ManyToOne(() => DataTable, (factTable) => factTable.dataTableDescriptions, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete'
  })
  @JoinColumn({ name: 'fact_table_id', foreignKeyConstraintName: 'FK_data_table_description_fact_table_id' })
  factTable: DataTable;
}
