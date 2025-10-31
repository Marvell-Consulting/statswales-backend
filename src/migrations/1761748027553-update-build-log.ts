import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateBuildLog1761748027553 implements MigrationInterface {
  name = 'UpdateBuildLog1761748027553';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "build_log" ADD "user_id" character varying`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "performance_start" double precision NOT NULL DEFAULT 0`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "performance_finish" double precision`);
    await queryRunner.query(`ALTER TABLE "build_log" ADD "duration" double precision`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "start_date" TIMESTAMP`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "end_date" TIMESTAMP`);
    await queryRunner.query(`ALTER TABLE "build_log" ALTER COLUMN "performance_start" DROP DEFAULT`);
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum" RENAME TO "build_log_type_enum_old"`);
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum" AS ENUM('base_cube', 'validation_cube', 'full_cube', 'all_cubes', 'draft_cubes')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum" USING "type"::"text"::"public"."build_log_type_enum"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum_old"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "user_id"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "performance_start"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "performance_finish"`);
    await queryRunner.query(`ALTER TABLE "build_log" DROP COLUMN "duration"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "start_date"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "end_date"`);
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum_old" AS ENUM('base_cube', 'validation_cube', 'full_cube')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum_old" USING "type"::"text"::"public"."build_log_type_enum_old"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum"`);
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum_old" RENAME TO "build_log_type_enum"`);
  }
}
