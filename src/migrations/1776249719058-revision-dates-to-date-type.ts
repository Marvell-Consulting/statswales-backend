import { MigrationInterface, QueryRunner } from 'typeorm';

export class RevisionDatesToDateType1776249719058 implements MigrationInterface {
  name = 'RevisionDatesToDateType1776249719058';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" ALTER COLUMN "start_date" TYPE date USING start_date::date`);
    await queryRunner.query(`ALTER TABLE "revision" ALTER COLUMN "end_date" TYPE date USING end_date::date`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "revision" ALTER COLUMN "start_date" TYPE timestamp without time zone USING start_date::timestamp`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ALTER COLUMN "end_date" TYPE timestamp without time zone USING end_date::timestamp`
    );
  }
}
