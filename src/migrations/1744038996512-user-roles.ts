import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserRoles1744038996512 implements MigrationInterface {
  name = 'UserRoles1744038996512';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IX_user_provider"`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_provider_provider_user_id"`);

    await queryRunner.query(
      `CREATE TABLE "user_group_role" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "roles" jsonb NOT NULL DEFAULT '[]', "user_id" uuid NOT NULL, "group_id" uuid NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_user_group_role_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_roles" ON "user_group_role" ("roles") `);
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_user_id" ON "user_group_role" ("user_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_group_id" ON "user_group_role" ("group_id") `);

    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "email_verified"`);

    await queryRunner.query(`ALTER TABLE "user" ADD "global_roles" jsonb NOT NULL DEFAULT '[]'`);
    await queryRunner.query(`CREATE TYPE "public"."user_status_enum" AS ENUM('active', 'inactive')`);
    await queryRunner.query(`ALTER TABLE "user" ADD "status" "public"."user_status_enum" NOT NULL DEFAULT 'active'`);

    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider" SET NOT NULL`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" TYPE text`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "email" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "email" SET NOT NULL`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "given_name" TYPE text`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "family_name" TYPE text`);

    await queryRunner.query(`CREATE INDEX "IDX_user_provider" ON "user" ("provider") `);
    await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
    await queryRunner.query(`CREATE INDEX "IDX_user_status" ON "user" ("status") `);
    await queryRunner.query(
      `CREATE INDEX "IDX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_role" ADD CONSTRAINT "FK_user_group_role_user_id" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_role" ADD CONSTRAINT "FK_user_group_role_group_id" FOREIGN KEY ("group_id") REFERENCES "user_group"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(`DROP TABLE IF EXISTS "user_group_user"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user_group_role" DROP CONSTRAINT "FK_user_group_role_group_id"`);
    await queryRunner.query(`ALTER TABLE "user_group_role" DROP CONSTRAINT "FK_user_group_role_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider_provider_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_status"`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider"`);

    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "family_name" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "given_name" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "email" TYPE character varying`);
    await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider" TYPE character varying`);
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "status"`);
    await queryRunner.query(`DROP TYPE "public"."user_status_enum"`);
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "global_roles"`);
    await queryRunner.query(`ALTER TABLE "user" ADD "email_verified" boolean NOT NULL DEFAULT false`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_group_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_roles"`);
    await queryRunner.query(`DROP TABLE "user_group_role"`);
    await queryRunner.query(
      `CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
    await queryRunner.query(`CREATE INDEX "IX_user_provider" ON "user" ("provider") `);
  }
}
