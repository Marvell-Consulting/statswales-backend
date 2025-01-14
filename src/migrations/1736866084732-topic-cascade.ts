import { MigrationInterface, QueryRunner } from 'typeorm';

export class TopicCascade1736866084732 implements MigrationInterface {
    name = 'TopicCascade1736866084732';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_dataset_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_dataset_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_topic" ADD CONSTRAINT "FK_dataset_topic_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }
}
