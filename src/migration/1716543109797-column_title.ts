import { MigrationInterface, QueryRunner } from 'typeorm';

export class ColumnTitle1716543109797 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
        CREATE TABLE IF NOT EXISTS "column_title" (
          "dataset_column_id" uuid NOT NULL,
          "title" varchar(4096) NOT NULL,
          "language_code" varchar(2) NOT NULL,
          CONSTRAINT "PK_name_datasetid_languageCode" PRIMARY KEY ("dataset_column_id", "language_code"),
          CONSTRAINT "FK_column_id_to_column_title_column" FOREIGN KEY(dataset_column_id) REFERENCES dataset_column(id)
         );
        `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "column_title"`, undefined);
    }
}
