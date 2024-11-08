import { MigrationInterface, QueryRunner } from 'typeorm';

export class DataProviders1731084525168 implements MigrationInterface {
    name = 'DataProviders1731084525168';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "dataset_provider" ("id" uuid NOT NULL, "dataset_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "provider_id" uuid NOT NULL, "provider_source_id" uuid, "datasetId" uuid, CONSTRAINT "PK_dataset_provider_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "provider_source" ("id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "provider_id" uuid NOT NULL, CONSTRAINT "PK_provider_source_id_language" PRIMARY KEY ("id", "language"))`
        );
        await queryRunner.query(
            `CREATE TABLE "provider" ("id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_provider_id_language" PRIMARY KEY ("id", "language"))`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_3fc1db8a7a5017deddbf85979ce" FOREIGN KEY ("datasetId") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" ADD CONSTRAINT "FK_dataset_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id","language") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "provider_source" ADD CONSTRAINT "FK_provider_source_provider_id" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "provider_source" DROP CONSTRAINT "FK_provider_source_provider_id"`);
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_source_id_language"`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_id_language"`
        );
        await queryRunner.query(`ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_3fc1db8a7a5017deddbf85979ce"`);
        await queryRunner.query(`DROP TABLE "provider"`);
        await queryRunner.query(`DROP TABLE "provider_source"`);
        await queryRunner.query(`DROP TABLE "dataset_provider"`);
    }
}
