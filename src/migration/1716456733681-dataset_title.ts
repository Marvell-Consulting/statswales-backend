import { MigrationInterface, QueryRunner } from 'typeorm';

export class DatasetTitle1716456733681 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
        CREATE TABLE IF NOT EXISTS "dataset_title" (
          "dataset_id" uuid NOT NULL,
          "title" varchar(4096) NOT NULL,
          "language_code" char(5) NOT NULL,
          CONSTRAINT "PK_dataset_languageCode" PRIMARY KEY ("dataset_id", "language_code"),
          CONSTRAINT "FK_dataset_to_dataset_name_dataset" FOREIGN KEY(dataset_id) REFERENCES datasets(id)
         );
        `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "dataset_title"`, undefined);
    }
}
