import { MigrationInterface, QueryRunner } from 'typeorm';

// Add on delete cascade to join tables
export class Cascade1738930610485 implements MigrationInterface {
    name = 'Cascade1738930610485';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_source_id_language"`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_id_language"`
        );
        await queryRunner.query(`ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_dataset_id"`);
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_source_id_language"`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_id_language"`
        );
        await queryRunner.query(`ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_dataset_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id","language") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }
}
