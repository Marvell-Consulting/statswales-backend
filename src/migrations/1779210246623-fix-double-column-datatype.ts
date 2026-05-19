import { MigrationInterface, QueryRunner } from 'typeorm';

export class FixDoubleColumnDatatype1779210246623 implements MigrationInterface {
  name = 'FixDoubleColumnDatatype1779210246623';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `UPDATE "fact_table_column" SET "column_datatype" = 'DOUBLE PRECISION' WHERE "column_datatype" = 'DOUBLE'`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `UPDATE "fact_table_column" SET "column_datatype" = 'DOUBLE' WHERE "column_datatype" = 'DOUBLE PRECISION'`
    );
  }
}
