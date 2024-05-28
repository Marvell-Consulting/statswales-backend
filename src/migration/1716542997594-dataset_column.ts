import { MigrationInterface, QueryRunner } from 'typeorm';

export class DatasetColumn1716542997594 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
        CREATE TABLE IF NOT EXISTS "dataset_column" (
          "dataset_id" uuid NOT NULL,
          "id" uuid NOT NULL DEFAULT gen_random_uuid(),
          "csv_title" varchar(255) NOT NULL,
          "type" varchar(10) NOT NULL,
          CONSTRAINT "PK_name_dataset_column_id" PRIMARY KEY ("id"),
          CONSTRAINT "FK_dataset_to_dataset_column_dataset" FOREIGN KEY(dataset_id) REFERENCES datasets(id)
         );
        `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "dataset_column"`, undefined);
    }
}
