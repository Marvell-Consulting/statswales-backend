import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserStatus1743434122392 implements MigrationInterface {
  name = 'UserStatus1743434122392';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."UX_user_provider_provider_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IX_user_provider"`);
    await queryRunner.query(`CREATE TYPE "public"."user_status_enum" AS ENUM('active', 'inactive')`);
    await queryRunner.query(`ALTER TABLE "user" ADD "status" "public"."user_status_enum" NOT NULL DEFAULT 'active'`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" DROP NOT NULL`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "email" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "given_name" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "family_name" TYPE text`);
    await queryRunner.query(`CREATE INDEX "IDX_user_provider" ON "user" ("provider") `);
    await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
    await queryRunner.query(`CREATE INDEX "IDX_user_status" ON "user" ("status") `);
    await queryRunner.query(
      `CREATE INDEX "IDX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider_provider_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_status"`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider"`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "family_name" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "given_name" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "email" TYPE character varying`);
    await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" SET NOT NULL`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "status"`);
    await queryRunner.query(`DROP TYPE "public"."user_status_enum"`);
    await queryRunner.query(`CREATE INDEX "IX_user_provider" ON "user" ("provider") `);
    await queryRunner.query(
      `CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
  }
}
