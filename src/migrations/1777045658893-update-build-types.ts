import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateBuildTypes1777045658893 implements MigrationInterface {
  name = 'UpdateBuildTypes1777045658893';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum" RENAME TO "build_log_type_enum_old"`);
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum" AS ENUM('base_cube', 'validation_cube', 'full_cube', 'all_cubes', 'all_filter_tables', 'draft_cubes')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum" USING "type"::"text"::"public"."build_log_type_enum"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum_old"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum_old" AS ENUM('base_cube', 'validation_cube', 'full_cube', 'all_cubes', 'draft_cubes')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum_old" USING "type"::"text"::"public"."build_log_type_enum_old"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum"`);
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum_old" RENAME TO "build_log_type_enum"`);
  }
}
