import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserGroup1743077319099 implements MigrationInterface {
  name = 'UserGroup1743077319099';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "dataset" ADD "user_group_id" uuid`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_team_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_dataset_team_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "team_id"`);
    await queryRunner.query(
      `CREATE TABLE "organisation_metadata" ("organisation_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_organisation_metadata_organisation_id_language" PRIMARY KEY ("organisation_id", "language"))`
    );
    await queryRunner.query(`CREATE TYPE "public"."user_group_status_enum" AS ENUM('active', 'inactive')`);
    await queryRunner.query(
      `CREATE TABLE "user_group" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "prefix" text, "organisation_id" uuid, "status" "public"."user_group_status_enum" NOT NULL DEFAULT 'active', "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_user_group_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_user_group_organisation_id" ON "user_group" ("organisation_id") `);
    await queryRunner.query(
      `CREATE TABLE "user_group_metadata" ("user_group_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text, "email" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_user_group_metadata_user_group_id_language" PRIMARY KEY ("user_group_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "user_group_user" ("user_group_id" uuid NOT NULL, "user_id" uuid NOT NULL, CONSTRAINT "PK_0242ea2578f42d4cf58f9c54f9d" PRIMARY KEY ("user_group_id", "user_id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_a0f3c964602f06fed4c9e678dc" ON "user_group_user" ("user_group_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_17dd1f7e1090bba508cc84f09a" ON "user_group_user" ("user_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_dataset_user_group_id" ON "dataset" ("user_group_id") `);
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_user_group_id" FOREIGN KEY ("user_group_id") REFERENCES "user_group"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "organisation_metadata" ADD CONSTRAINT "FK_organisation_metadata_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group" ADD CONSTRAINT "FK_user_group_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_metadata" ADD CONSTRAINT "FK_user_group_metadata_user_group_id" FOREIGN KEY ("user_group_id") REFERENCES "user_group"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_user" ADD CONSTRAINT "FK_user_group_user_user_group_id" FOREIGN KEY ("user_group_id") REFERENCES "user_group"("id") ON DELETE CASCADE ON UPDATE CASCADE`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_user" ADD CONSTRAINT "FK_user_group_user_user_id" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );

    // drop old tables
    await queryRunner.query(`DROP TABLE IF EXISTS "team_info"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "team"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "organisation_info"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user_group_user" DROP CONSTRAINT "FK_user_group_user_user_id"`);
    await queryRunner.query(`ALTER TABLE "user_group_user" DROP CONSTRAINT "FK_user_group_user_user_group_id"`);
    await queryRunner.query(`ALTER TABLE "user_group_metadata" DROP CONSTRAINT "FK_user_group_metadata_user_group_id"`);
    await queryRunner.query(`ALTER TABLE "user_group" DROP CONSTRAINT "FK_user_group_organisation_id"`);
    await queryRunner.query(
      `ALTER TABLE "organisation_metadata" DROP CONSTRAINT "FK_organisation_metadata_organisation_id"`
    );
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_user_group_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_dataset_user_group_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_17dd1f7e1090bba508cc84f09a"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_a0f3c964602f06fed4c9e678dc"`);
    await queryRunner.query(`DROP TABLE "user_group_user"`);
    await queryRunner.query(`DROP TABLE "user_group_metadata"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_organisation_id"`);
    await queryRunner.query(`DROP TABLE "user_group"`);
    await queryRunner.query(`DROP TYPE "public"."user_group_status_enum"`);
    await queryRunner.query(`DROP TABLE "organisation_metadata"`);
    await queryRunner.query(`ALTER TABLE "dataset" RENAME COLUMN "user_group_id" TO "team_id"`);
    await queryRunner.query(`CREATE INDEX "IDX_dataset_team_id" ON "dataset" ("team_id") `);
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_team_id" FOREIGN KEY ("team_id") REFERENCES "team"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }
}
