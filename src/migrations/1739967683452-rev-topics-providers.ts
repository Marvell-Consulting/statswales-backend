import { MigrationInterface, QueryRunner } from 'typeorm';

export class RevTopicsProviders1739967683452 implements MigrationInterface {
    name = 'RevTopicsProviders1739967683452';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "revision_provider" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "group_id" uuid NOT NULL, "revision_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "provider_id" uuid NOT NULL, "provider_source_id" uuid, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_revision_provider_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE INDEX "IDX_revision_provider_revision_id" ON "revision_provider" ("revision_id") `
        );
        await queryRunner.query(
            `CREATE INDEX "IDX_revision_provider_provider_id_language" ON "revision_provider" ("provider_id", "language") `
        );
        await queryRunner.query(
            `CREATE INDEX "IDX_revision_provider_provider_source_id_language" ON "revision_provider" ("provider_source_id", "language") `
        );
        await queryRunner.query(
            `CREATE TABLE "revision_topic" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "revision_id" uuid NOT NULL, "topic_id" integer NOT NULL, CONSTRAINT "PK_revision_topic_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(`CREATE INDEX "IDX_revision_topic_revision_id" ON "revision_topic" ("revision_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_revision_topic_topic_id" ON "revision_topic" ("topic_id") `);
        await queryRunner.query(
            `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision_topic" ADD CONSTRAINT "FK_revision_topic_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision_topic" ADD CONSTRAINT "FK_revision_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "revision_topic" DROP CONSTRAINT "FK_revision_topic_topic_id"`);
        await queryRunner.query(`ALTER TABLE "revision_topic" DROP CONSTRAINT "FK_revision_topic_revision_id"`);
        await queryRunner.query(
            `ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_provider_source_id_language"`
        );
        await queryRunner.query(
            `ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_provider_id_language"`
        );
        await queryRunner.query(`ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_revision_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revision_topic_topic_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revision_topic_revision_id"`);
        await queryRunner.query(`DROP TABLE "revision_topic"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_provider_source_id_language"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_provider_id_language"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_revision_id"`);
        await queryRunner.query(`DROP TABLE "revision_provider"`);
    }
}
