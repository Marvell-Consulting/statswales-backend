import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1728642196537 implements MigrationInterface {
    name = 'InitialSchema1728642196537';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            CREATE TABLE "category" (
                "category" text NOT NULL,
                CONSTRAINT "PK_dab3b9cd30b5940f3a808316991" PRIMARY KEY ("category")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "category_key" (
                "category_key" text NOT NULL,
                "category" text NOT NULL,
                CONSTRAINT "PK_b305284188b72bbeb54babee1c8" PRIMARY KEY ("category_key")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "reference_data" (
                "item_id" text NOT NULL,
                "version_no" integer NOT NULL,
                "category_key" text NOT NULL,
                "sort_order" integer,
                "validity_start" date NOT NULL,
                "validity_end" date,
                CONSTRAINT "PK_1c127907e0b334cd1cc15afc1bb" PRIMARY KEY ("item_id", "version_no", "category_key")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "reference_data_info" (
                "item_id" text NOT NULL,
                "version_no" integer NOT NULL,
                "category_key" text NOT NULL,
                "lang" text NOT NULL,
                "description" text NOT NULL,
                "notes" text,
                CONSTRAINT "PK_bc5f1f5cf97870b0d373f7edae7" PRIMARY KEY ("item_id", "version_no", "category_key", "lang")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "category_key_info" (
                "category_key" text NOT NULL,
                "lang" text NOT NULL,
                "description" text NOT NULL,
                "notes" text,
                CONSTRAINT "PK_845dcc2857b4a2d31252e03f8d5" PRIMARY KEY ("category_key", "lang")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "user" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "provider" character varying NOT NULL,
                "provider_user_id" character varying NOT NULL,
                "email" character varying NOT NULL,
                "email_verified" boolean NOT NULL DEFAULT false,
                "given_name" character varying,
                "family_name" character varying,
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_user_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE INDEX "IX_user_provider" ON "user" ("provider")
        `);
        await queryRunner.query(`
            CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email")
        `);
        await queryRunner.query(`
            CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id")
        `);
        await queryRunner.query(`
            CREATE TABLE "csv_info" (
                "import_id" uuid NOT NULL,
                "delimiter" character varying(1) NOT NULL,
                "quote" character varying(1) NOT NULL,
                "linebreak" character varying(2) NOT NULL,
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_csv_info_import_id" PRIMARY KEY ("import_id")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."file_import_type_enum" AS ENUM('draft', 'fact_table', 'lookup_table')
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."file_import_location_enum" AS ENUM('blob_storage', 'data_lake', 'unknown')
        `);
        await queryRunner.query(`
            CREATE TABLE "file_import" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "mime_type" character varying(255) NOT NULL,
                "filename" character varying(255) NOT NULL,
                "hash" character varying(255) NOT NULL,
                "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "type" "public"."file_import_type_enum" NOT NULL,
                "location" "public"."file_import_location_enum" NOT NULL,
                "revision_id" uuid,
                CONSTRAINT "PK_import_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "revision" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "revision_index" integer NOT NULL,
                "online_cube_filename" character varying(255),
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "approved_at" TIMESTAMP WITH TIME ZONE,
                "publish_at" TIMESTAMP WITH TIME ZONE,
                "dataset_id" uuid,
                "previous_revision_id" uuid,
                "created_by" uuid,
                "approved_by" uuid,
                CONSTRAINT "PK_revision_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dataset_info" (
                "dataset_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "title" text,
                "description" text,
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_dataset_info_dataset_id_language" PRIMARY KEY ("dataset_id", "language")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dataset" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "live" TIMESTAMP WITH TIME ZONE,
                "archive" TIMESTAMP WITH TIME ZONE,
                "created_by" uuid,
                CONSTRAINT "PK_dataset_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dimension_info" (
                "dimension_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "name" text NOT NULL,
                "description" text,
                "notes" text,
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_dimension_info_dimension_id_language" PRIMARY KEY ("dimension_id", "language")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."dimension_type_enum" AS ENUM(
                'raw',
                'text',
                'numeric',
                'symbol',
                'lookup_table',
                'time_period',
                'time_point',
                'footnote'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dimension" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "type" "public"."dimension_type_enum" NOT NULL,
                "validator" text,
                "dataset_id" uuid,
                "start_revision_id" uuid,
                "finish_revision_id" uuid,
                CONSTRAINT "PK_dimension_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."source_action_enum" AS ENUM(
                'create',
                'append',
                'truncate_then_load',
                'ignore',
                'unknown'
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."source_type_enum" AS ENUM(
                'data_values',
                'foot_notes',
                'dimension',
                'ignore',
                'unknown'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "source" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "column_index" integer NOT NULL,
                "csv_field" text NOT NULL,
                "action" "public"."source_action_enum",
                "type" "public"."source_type_enum",
                "dimension_id" uuid,
                "import_id" uuid NOT NULL,
                "revision_id" uuid,
                CONSTRAINT "PK_source_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "hierarchy" (
                "item_id" text NOT NULL,
                "version_no" integer NOT NULL,
                "category_key" text NOT NULL,
                "parent_id" text NOT NULL,
                "parent_version" integer NOT NULL,
                "parent_category" text NOT NULL,
                CONSTRAINT "PK_229e83ef14da2fd389c20a881c9" PRIMARY KEY (
                    "item_id",
                    "version_no",
                    "category_key",
                    "parent_id",
                    "parent_version",
                    "parent_category"
                )
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "category_info" (
                "category" text NOT NULL,
                "lang" text NOT NULL,
                "description" text NOT NULL,
                "notes" text,
                CONSTRAINT "PK_b352b1990fc76e4cb4c7c9e0c9d" PRIMARY KEY ("category", "lang")
            )
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key"
            ADD CONSTRAINT "FK_087b36846d67092609821a62756" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data"
            ADD CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data_info"
            ADD CONSTRAINT "FK_f671fde9c769286ba971485ba09" FOREIGN KEY ("item_id", "version_no", "category_key") REFERENCES "reference_data"("item_id", "version_no", "category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info"
            ADD CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "csv_info"
            ADD CONSTRAINT "FK_csv_info_import_id" FOREIGN KEY ("import_id") REFERENCES "file_import"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "file_import"
            ADD CONSTRAINT "FK_import_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "revision"
            ADD CONSTRAINT "FK_revision_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "revision"
            ADD CONSTRAINT "FK_revision_previous_revision_id" FOREIGN KEY ("previous_revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "revision"
            ADD CONSTRAINT "FK_revision_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "revision"
            ADD CONSTRAINT "FK_revision_approved_by" FOREIGN KEY ("approved_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_info"
            ADD CONSTRAINT "FK_dataset_info_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset"
            ADD CONSTRAINT "FK_dataset_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension_info"
            ADD CONSTRAINT "FK_dimension_info_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_start_revision_id" FOREIGN KEY ("start_revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_finish_revision_id" FOREIGN KEY ("finish_revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "source"
            ADD CONSTRAINT "FK_source_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "source"
            ADD CONSTRAINT "FK_source_import_id" FOREIGN KEY ("import_id") REFERENCES "file_import"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "source"
            ADD CONSTRAINT "FK_source_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy"
            ADD CONSTRAINT "FK_aaba494e02eaa91111d549e5763" FOREIGN KEY ("item_id", "version_no", "category_key") REFERENCES "reference_data"("item_id", "version_no", "category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy"
            ADD CONSTRAINT "FK_6ca6a866371cdd67a638df8a74c" FOREIGN KEY ("parent_id", "parent_version", "parent_category") REFERENCES "reference_data"("item_id", "version_no", "category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_info"
            ADD CONSTRAINT "FK_68028565126809c1e925e6f9334" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "category_info" DROP CONSTRAINT "FK_68028565126809c1e925e6f9334"
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy" DROP CONSTRAINT "FK_6ca6a866371cdd67a638df8a74c"
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy" DROP CONSTRAINT "FK_aaba494e02eaa91111d549e5763"
        `);
        await queryRunner.query(`
            ALTER TABLE "source" DROP CONSTRAINT "FK_source_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "source" DROP CONSTRAINT "FK_source_import_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "source" DROP CONSTRAINT "FK_source_dimension_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_finish_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_start_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension_info" DROP CONSTRAINT "FK_dimension_info_dimension_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_created_by"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_info" DROP CONSTRAINT "FK_dataset_info_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_approved_by"
        `);
        await queryRunner.query(`
            ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_created_by"
        `);
        await queryRunner.query(`
            ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_previous_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "file_import" DROP CONSTRAINT "FK_import_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "csv_info" DROP CONSTRAINT "FK_csv_info_import_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47"
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data_info" DROP CONSTRAINT "FK_f671fde9c769286ba971485ba09"
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data" DROP CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key" DROP CONSTRAINT "FK_087b36846d67092609821a62756"
        `);
        await queryRunner.query(`
            DROP TABLE "category_info"
        `);
        await queryRunner.query(`
            DROP TABLE "hierarchy"
        `);
        await queryRunner.query(`
            DROP TABLE "source"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."source_type_enum"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."source_action_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "dimension"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."dimension_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "dimension_info"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset_info"
        `);
        await queryRunner.query(`
            DROP TABLE "revision"
        `);
        await queryRunner.query(`
            DROP TABLE "file_import"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."file_import_location_enum"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."file_import_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "csv_info"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."UX_user_provider_provider_user_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."UX_user_email"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IX_user_provider"
        `);
        await queryRunner.query(`
            DROP TABLE "user"
        `);
        await queryRunner.query(`
            DROP TABLE "category_key_info"
        `);
        await queryRunner.query(`
            DROP TABLE "reference_data_info"
        `);
        await queryRunner.query(`
            DROP TABLE "reference_data"
        `);
        await queryRunner.query(`
            DROP TABLE "category_key"
        `);
        await queryRunner.query(`
            DROP TABLE "category"
        `);
    }
}
