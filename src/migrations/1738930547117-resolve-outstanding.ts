import { MigrationInterface, QueryRunner } from 'typeorm';

// Not sure why, but these are showing as outstanding changes when I run migration:generate
export class ResolveOutstanding1738930547117 implements MigrationInterface {
    name = 'ResolveOutstanding1738930547117';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE SEQUENCE IF NOT EXISTS "event_log_id_seq" OWNED BY "event_log"."id"`);
        await queryRunner.query(`ALTER TABLE "event_log" ALTER COLUMN "id" SET DEFAULT nextval('"event_log_id_seq"')`);
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"`);
        await queryRunner.query(`CREATE SEQUENCE IF NOT EXISTS "topic_id_seq" OWNED BY "topic"."id"`);
        await queryRunner.query(`ALTER TABLE "topic" ALTER COLUMN "id" SET DEFAULT nextval('"topic_id_seq"')`);
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"`);
        await queryRunner.query(`ALTER TABLE "topic" ALTER COLUMN "id" DROP DEFAULT`);
        await queryRunner.query(`DROP SEQUENCE "topic_id_seq"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(`ALTER TABLE "event_log" ALTER COLUMN "id" DROP DEFAULT`);
        await queryRunner.query(`DROP SEQUENCE "event_log_id_seq"`);
    }
}
