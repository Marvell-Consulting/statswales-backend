import { MigrationInterface, QueryRunner } from 'typeorm';

export class Topics1732554666075 implements MigrationInterface {
    name = 'Topics1732554666075';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "topic" ("id" SERIAL NOT NULL, "path" ltree NOT NULL, "name_en" text, "name_cy" text, CONSTRAINT "PK_topic_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "dataset_topic" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "dataset_id" uuid NOT NULL, "topic_id" integer NOT NULL, CONSTRAINT "PK_dataset_topic_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"`);
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_dataset_id"`);
        await queryRunner.query(`DROP TABLE "dataset_topic"`);
        await queryRunner.query(`DROP TABLE "topic"`);
    }
}
