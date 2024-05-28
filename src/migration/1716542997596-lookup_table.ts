import { MigrationInterface, QueryRunner } from 'typeorm';

export class LookupTable1716542997596 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
        CREATE TABLE IF NOT EXISTS "lookup_tables" (
              "id" uuid NOT NULL DEFAULT gen_random_uuid(),
              "sha256hash" varchar(255) NOT NULL,
              "dataset_id" uuid NOT NULL,
              "creation_date" TIMESTAMP NOT NULL DEFAULT now(),
              "created_by" varchar(255) NULL,
              "last_modified" TIMESTAMP NOT NULL DEFAULT now(),
              "modified_by" varchar(255) NULL,
              "dataset_column_id" uuid NULL,
              CONSTRAINT "PK_lookup_tables_id" PRIMARY KEY ("id"),
              CONSTRAINT "FK_dataset_to_lookup_tables_dataset" FOREIGN KEY(dataset_id) REFERENCES datasets(id),
              CONSTRAINT "FK_dataset_column_id_to_lookup_tables_dataset_column_id" FOREIGN KEY(dataset_column_id) REFERENCES dataset_column(id)
          );
          `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "lookup_tbles"`, undefined);
    }
}
