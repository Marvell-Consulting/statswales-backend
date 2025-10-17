import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateBuildLogEnum1760710327980 implements MigrationInterface {
  name = 'UpdateBuildLogEnum1760710327980';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum" RENAME TO "build_log_type_enum_old"`);
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum" AS ENUM('base_cube', 'validation_cube', 'full_cube')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum" USING "type"::"text"::"public"."build_log_type_enum"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum_old"`);
    await queryRunner.query('CREATE EXTENSION IF NOT EXISTS tablefunc;');
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum_old" AS ENUM('baseCube', 'validationCube', 'fullCube')`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ALTER COLUMN "type" TYPE "public"."build_log_type_enum_old" USING "type"::"text"::"public"."build_log_type_enum_old"`
    );
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum"`);
    await queryRunner.query(`ALTER TYPE "public"."build_log_type_enum_old" RENAME TO "build_log_type_enum"`);
  }
}
