import { MigrationInterface, QueryRunner } from 'typeorm';

export class Datafiles1716456713793 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
        CREATE TABLE IF NOT EXISTS "datafiles" (
               "id" uuid NOT NULL DEFAULT gen_random_uuid(),
               "sha256hash" varchar(255) NOT NULL,
               "dataset_id" uuid NOT NULL,
               "creation_date" TIMESTAMP NOT NULL DEFAULT now(),
               "created_by" varchar(255) NULL,
               "last_modified" TIMESTAMP NOT NULL DEFAULT now(),
               "modified_by" varchar(255) NULL,
               CONSTRAINT "PK_datafiles_id" PRIMARY KEY ("id"),
               CONSTRAINT "FK_dataset_to_datafile_dataset" FOREIGN KEY(dataset_id) REFERENCES datasets(id)
          )
        `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "datafiles"`, undefined);
    }
}
