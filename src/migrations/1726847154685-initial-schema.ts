import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1726847154685 implements MigrationInterface {
    name = 'InitialSchema1726847154685';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "users" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "provider" character varying NOT NULL, "provider_user_id" character varying NOT NULL, "email" character varying NOT NULL, "email_verified" boolean NOT NULL DEFAULT false, "given_name" character varying, "family_name" character varying, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_user_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(`CREATE INDEX "IX_user_provider" ON "users" ("provider") `);
        await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "users" ("email") `);
        await queryRunner.query(
            `CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "users" ("provider", "provider_user_id") `
        );
        await queryRunner.query(
            `CREATE TABLE "csv_info" ("import_id" uuid NOT NULL, "delimiter" character varying(1) NOT NULL, "quote" character varying(1) NOT NULL, "linebreak" character varying(2) NOT NULL, CONSTRAINT "PK_csv_info_import_id" PRIMARY KEY ("import_id"))`
        );
        await queryRunner.query(`CREATE TYPE "public"."import_type_enum" AS ENUM('Draft', 'FactTable', 'LookupTable')`);
        await queryRunner.query(
            `CREATE TYPE "public"."import_location_enum" AS ENUM('BlobStorage', 'Datalake', 'Unknown')`
        );
        await queryRunner.query(
            `CREATE TABLE "import" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "mime_type" character varying(255) NOT NULL, "filename" character varying(255) NOT NULL, "hash" character varying(255) NOT NULL, "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "type" "public"."import_type_enum" NOT NULL, "location" "public"."import_location_enum" NOT NULL, "revision_id" uuid, CONSTRAINT "PK_import_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "revision" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "revisionIndex" integer NOT NULL, "creation_date" TIMESTAMP NOT NULL DEFAULT now(), "online_cube_filename" character varying(255), "publish_date" TIMESTAMP WITH TIME ZONE, "approval_date" TIMESTAMP WITH TIME ZONE, "dataset_id" uuid, "previous_revision_id" uuid, "approved_by" uuid, "created_by" uuid, CONSTRAINT "PK_revision_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "dataset_info" ("dataset_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "title" text, "description" text, CONSTRAINT "PK_dataset_info_dataset_id_language" PRIMARY KEY ("dataset_id", "language"))`
        );
        await queryRunner.query(
            `CREATE TABLE "dataset" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "creation_date" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "live" TIMESTAMP WITH TIME ZONE, "archive" TIMESTAMP WITH TIME ZONE, "created_by" uuid, CONSTRAINT "PK_dataset_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "dimension_info" ("dimension_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "description" text, "notes" text, CONSTRAINT "PK_dimension_info_dimension_id_language" PRIMARY KEY ("dimension_id", "language"))`
        );
        await queryRunner.query(
            `CREATE TYPE "public"."dimension_type_enum" AS ENUM('RAW', 'TEXT', 'NUMERIC', 'SYMBOL', 'LOOKUP_TABLE', 'TIME_PERIOD', 'TIME_POINT')`
        );
        await queryRunner.query(
            `CREATE TABLE "dimension" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "type" "public"."dimension_type_enum" NOT NULL, "validator" text, "dataset_id" uuid, "start_revision_id" uuid, "finish_revision_id" uuid, CONSTRAINT "PK_dimension_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TYPE "public"."source_action_enum" AS ENUM('create', 'append', 'truncate-then-load', 'ignore')`
        );
        await queryRunner.query(
            `CREATE TABLE "source" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "column_index" integer NOT NULL, "csv_field" text NOT NULL, "action" "public"."source_action_enum" NOT NULL, "dimension_id" uuid, "import_id" uuid NOT NULL, "revision_id" uuid, CONSTRAINT "PK_source_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `ALTER TABLE "csv_info" ADD CONSTRAINT "FK_csv_info_import_id" FOREIGN KEY ("import_id") REFERENCES "import"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "import" ADD CONSTRAINT "FK_import_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_previous_revision_id" FOREIGN KEY ("previous_revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_approved_by" FOREIGN KEY ("approved_by") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_created_by" FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_info" ADD CONSTRAINT "FK_dataset_info_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_created_by" FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension_info" ADD CONSTRAINT "FK_dimension_info_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_start_revision_id" FOREIGN KEY ("start_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_finish_revision_id" FOREIGN KEY ("finish_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "source" ADD CONSTRAINT "FK_source_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "source" ADD CONSTRAINT "FK_source_import_id" FOREIGN KEY ("import_id") REFERENCES "import"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "source" ADD CONSTRAINT "FK_source_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "source" DROP CONSTRAINT "FK_source_revision_id"`);
        await queryRunner.query(`ALTER TABLE "source" DROP CONSTRAINT "FK_source_import_id"`);
        await queryRunner.query(`ALTER TABLE "source" DROP CONSTRAINT "FK_source_dimension_id"`);
        await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_finish_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_start_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_dataset_id"`);
        await queryRunner.query(`ALTER TABLE "dimension_info" DROP CONSTRAINT "FK_dimension_info_dimension_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_created_by"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP CONSTRAINT "FK_dataset_info_dataset_id"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_created_by"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_approved_by"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_previous_revision_id"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_dataset_id"`);
        await queryRunner.query(`ALTER TABLE "import" DROP CONSTRAINT "FK_import_revision_id"`);
        await queryRunner.query(`ALTER TABLE "csv_info" DROP CONSTRAINT "FK_csv_info_import_id"`);
        await queryRunner.query(`DROP TABLE "source"`);
        await queryRunner.query(`DROP TYPE "public"."source_action_enum"`);
        await queryRunner.query(`DROP TABLE "dimension"`);
        await queryRunner.query(`DROP TYPE "public"."dimension_type_enum"`);
        await queryRunner.query(`DROP TABLE "dimension_info"`);
        await queryRunner.query(`DROP TABLE "dataset"`);
        await queryRunner.query(`DROP TABLE "dataset_info"`);
        await queryRunner.query(`DROP TABLE "revision"`);
        await queryRunner.query(`DROP TABLE "import"`);
        await queryRunner.query(`DROP TYPE "public"."import_location_enum"`);
        await queryRunner.query(`DROP TYPE "public"."import_type_enum"`);
        await queryRunner.query(`DROP TABLE "csv_info"`);
        await queryRunner.query(`DROP INDEX "public"."UX_user_provider_provider_user_id"`);
        await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
        await queryRunner.query(`DROP INDEX "public"."IX_user_provider"`);
        await queryRunner.query(`DROP TABLE "users"`);
    }
}
