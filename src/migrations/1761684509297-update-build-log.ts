import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateBuildLog1761684509297 implements MigrationInterface {
  name = 'UpdateBuildLog1761684509297';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "build_log" ADD "user_id" character varying`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "performance_start" double precision NOT NULL DEFAULT 0`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "performance_finish" double precision`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "duration" double precision`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "start_date" TIMESTAMP`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "end_date" TIMESTAMP`);
    await queryRunner.query(`ALTER TABLE "build_log" ALTER COLUMN "performance_start" DROP DEFAULT`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "end_date"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "start_date"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "duration"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "performance_finish"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "performance_start"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "user_id"`);
  }
}
