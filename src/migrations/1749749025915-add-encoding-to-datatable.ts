import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddEncodingToDatatable1749749025915 implements MigrationInterface {
  name = 'AddEncodingToDatatable1749749025915';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "data_table" ADD "encoding" character varying`);
    await queryRunner.query(`ALTER TABLE "lookup_table" ADD "encoding" character varying`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "lookup_table" DROP COLUMN "encoding"`);
    await queryRunner.query(`ALTER TABLE "data_table" DROP COLUMN "encoding"`);
  }
}
