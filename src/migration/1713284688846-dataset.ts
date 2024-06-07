import { MigrationInterface, QueryRunner } from 'typeorm';

export class Dataset1713284688846 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `
          --Table Definition
          CREATE TABLE IF NOT EXISTS "datasets"  (
            "id" uuid NOT NULL DEFAULT gen_random_uuid(),
            "internal_name" varchar(255) NOT NULL,
            "creation_date" TIMESTAMP NOT NULL DEFAULT now(),
            "created_by" varchar(255) NULL,
            "last_modified" TIMESTAMP NOT NULL DEFAULT now(),
            "modified_by" varchar(255) NULL,
            "publish_date" TIMESTAMP NULL,
            "published_by" varchar(255) null,
            "live" BOOLEAN default false,
            "approved_by" varchar(255) null,
            "code" varchar(12) NULL,
            CONSTRAINT "PK_dataset_id" PRIMARY KEY ("id")
          );
        `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "dataset"`, undefined);
    }
}
