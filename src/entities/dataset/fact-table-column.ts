import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

import { FactTableColumnType } from '../../enums/fact-table-column-type';

import { Dataset } from './dataset';

@Entity({ name: 'fact_table_column' })
export class FactTableColumn extends BaseEntity {
  @PrimaryColumn({
    name: 'dataset_id',
    type: 'uuid',
    primaryKeyConstraintName: 'PK_fact_table_column_id_column_name'
  })
  id: string;

  @PrimaryColumn({
    name: 'column_name',
    type: 'varchar',
    primaryKeyConstraintName: 'PK_fact_table_column_id_column_name'
  })
  columnName: string;

  @Column({ name: 'column_type', type: 'enum', enum: Object.values(FactTableColumnType), nullable: false })
  columnType: FactTableColumnType;

  @Column({ name: 'column_datatype', type: 'varchar', nullable: false })
  columnDatatype: string;

  @Column({ name: 'column_index', type: 'integer', nullable: false })
  columnIndex: number;

  @ManyToOne(() => Dataset, (dataset) => dataset.factTable, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete'
  })
  @JoinColumn({ name: 'dataset_id', foreignKeyConstraintName: 'FK_dataset_id_fact_table_column_id' })
  dataset: Dataset;
}
