import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddPivotLineCount1772115892047 implements MigrationInterface {
  name = 'AddPivotLineCount1772115892047';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "query_store" ADD "total_pivot_lines" integer`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "query_store" DROP COLUMN "total_pivot_lines"`);
  }
}
