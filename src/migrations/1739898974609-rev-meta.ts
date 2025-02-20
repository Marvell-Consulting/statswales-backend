import { MigrationInterface, QueryRunner } from 'typeorm';

export class RevMeta1739898974609 implements MigrationInterface {
    name = 'RevMeta1739898974609';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "revision_metadata" ("revision_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "title" text, "summary" text, "collection" text, "quality" text, "rounding_description" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_revision_metadata_revision_id_language" PRIMARY KEY ("revision_id", "language"))`
        );
        await queryRunner.query(`ALTER TABLE "revision" ADD "rounding_applied" boolean`);
        await queryRunner.query(`ALTER TABLE "revision" ADD "update_frequency" text`);
        await queryRunner.query(
            `CREATE TYPE "public"."revision_designation_enum" AS ENUM('official', 'accredited', 'in_development', 'none')`
        );
        await queryRunner.query(`ALTER TABLE "revision" ADD "designation" "public"."revision_designation_enum"`);
        await queryRunner.query(`ALTER TABLE "revision" ADD "related_links" jsonb`);
        await queryRunner.query(
            `ALTER TABLE "revision_metadata" ADD CONSTRAINT "FK_revision_metadata_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "revision_metadata" DROP CONSTRAINT "FK_revision_metadata_revision_id"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "related_links"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "designation"`);
        await queryRunner.query(`DROP TYPE "public"."revision_designation_enum"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "update_frequency"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "rounding_applied"`);
        await queryRunner.query(`DROP TABLE "revision_metadata"`);
    }
}
