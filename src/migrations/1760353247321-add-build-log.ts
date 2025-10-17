import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddBuildLog1760353247321 implements MigrationInterface {
  name = 'AddBuildLog1760353247321';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_status_enum" AS ENUM('queued', 'building', 'failed', 'schema_rename', 'materializing', 'completed')`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."build_log_type_enum" AS ENUM('baseCube', 'validationCube', 'fullCube')`
    );
    await queryRunner.query(
      `CREATE TABLE "build_log" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "status" "public"."build_log_status_enum" NOT NULL, "type" "public"."build_log_type_enum" NOT NULL, "started_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "completed_at" TIMESTAMP WITH TIME ZONE, "build_script" text, "errors" text, "revision_id" uuid, CONSTRAINT "PK_build_log_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `ALTER TABLE "build_log" ADD CONSTRAINT "FK_revision_build_log_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "build_log" DROP CONSTRAINT "FK_revision_build_log_id"`);
    await queryRunner.query(`DROP TABLE "build_log"`);
    await queryRunner.query(`DROP TYPE "public"."build_log_type_enum"`);
    await queryRunner.query(`DROP TYPE "public"."build_log_status_enum"`);
  }
}
