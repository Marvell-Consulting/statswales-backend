import { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1732832523588 implements MigrationInterface {
    name = 'InitialSchema1732832523588';

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
            CREATE TABLE "category_key_info" (
                "category_key" text NOT NULL,
                "lang" text NOT NULL,
                "description" text NOT NULL,
                "notes" text,
                CONSTRAINT "PK_845dcc2857b4a2d31252e03f8d5" PRIMARY KEY ("category_key", "lang")
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
            CREATE TYPE "public"."dataset_info_designation_enum" AS ENUM(
                'official',
                'accredited',
                'in_development',
                'none'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dataset_info" (
                "dataset_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "title" text,
                "description" text,
                "collection" text,
                "quality" text,
                "rounding_applied" boolean,
                "rounding_description" text,
                "related_links" jsonb,
                "update_frequency" text,
                "designation" "public"."dataset_info_designation_enum",
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_dataset_info_dataset_id_language" PRIMARY KEY ("dataset_id", "language")
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
            CREATE TYPE "public"."measure_info_display_type_enum" AS ENUM(
                'DECIMAL',
                'DOUBLE',
                'INTEGER',
                'BIGINT',
                'PERCENT',
                'VARCHAR',
                'BOOLEAN',
                'DATE',
                'DATETIME',
                'TIME',
                'TIMESTAMP'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "measure_info" (
                "measure_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "description" character varying NOT NULL,
                "reference" character varying NOT NULL,
                "notes" text,
                "display_type" "public"."measure_info_display_type_enum" NOT NULL,
                CONSTRAINT "PK_measure_info_measure_id_language" PRIMARY KEY ("measure_id", "language")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "measure" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "fact_table_column" character varying NOT NULL,
                "join_column" character varying,
                "dataset_id" uuid,
                "lookup_table_id" uuid,
                CONSTRAINT "REL_d587d9925390a0bddf29598e9b" UNIQUE ("dataset_id"),
                CONSTRAINT "REL_08275fce5c8ccabbe8b82e2197" UNIQUE ("lookup_table_id"),
                CONSTRAINT "PK_measure_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."lookup_table_filetype_enum" AS ENUM(
                'csv',
                'parquet',
                'json',
                'xlsx',
                'csv.gz',
                'json.gz',
                'unknown'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "lookup_table" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "mime_type" character varying(255) NOT NULL,
                "filetype" "public"."lookup_table_filetype_enum" NOT NULL,
                "filename" character varying(255) NOT NULL,
                "hash" character varying(255) NOT NULL,
                "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "delimiter" character NOT NULL,
                "quote" character NOT NULL,
                "linebreak" character varying NOT NULL,
                CONSTRAINT "PK_lookup_table_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."dimension_type_enum" AS ENUM(
                'raw',
                'text',
                'numeric',
                'symbol',
                'lookup_table',
                'reference_data',
                'time_period',
                'time_point',
                'note_codes'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dimension" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "type" "public"."dimension_type_enum" NOT NULL,
                "extractor" jsonb,
                "join_column" character varying,
                "fact_table_column" character varying NOT NULL,
                "is_slice_dimension" boolean NOT NULL DEFAULT false,
                "dataset_id" uuid,
                "lookup_table_id" uuid,
                CONSTRAINT "REL_aa21260a923de02687ee91ef73" UNIQUE ("lookup_table_id"),
                CONSTRAINT "PK_dimension_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "provider_source" (
                "id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "sw2_id" integer,
                "name" text NOT NULL,
                "provider_id" uuid NOT NULL,
                CONSTRAINT "PK_provider_source_id_language" PRIMARY KEY ("id", "language")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "provider" (
                "id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "name" text NOT NULL,
                CONSTRAINT "PK_provider_id_language" PRIMARY KEY ("id", "language")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dataset_provider" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "group_id" uuid NOT NULL,
                "dataset_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "provider_id" uuid NOT NULL,
                "provider_source_id" uuid,
                "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_dataset_provider_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "topic" (
                "id" SERIAL NOT NULL,
                "path" text NOT NULL,
                "name_en" text,
                "name_cy" text,
                CONSTRAINT "PK_topic_id" PRIMARY KEY ("id")
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "dataset_topic" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "dataset_id" uuid NOT NULL,
                "topic_id" integer NOT NULL,
                CONSTRAINT "PK_dataset_topic_id" PRIMARY KEY ("id")
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
            CREATE TYPE "public"."fact_table_info_column_type_enum" AS ENUM(
                'data_values',
                'note_codes',
                'dimension',
                'measure',
                'time',
                'ignore',
                'unknown',
                'line_number'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "fact_table_info" (
                "fact_table_id" uuid NOT NULL,
                "column_name" character varying NOT NULL,
                "column_index" integer NOT NULL,
                "column_type" "public"."fact_table_info_column_type_enum" NOT NULL,
                CONSTRAINT "PK_fact_table_info_id_language" PRIMARY KEY ("fact_table_id", "column_name")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."fact_table_filetype_enum" AS ENUM(
                'csv',
                'parquet',
                'json',
                'xlsx',
                'csv.gz',
                'json.gz',
                'unknown'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "fact_table" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "mime_type" character varying(255) NOT NULL,
                "filetype" "public"."fact_table_filetype_enum" NOT NULL,
                "filename" character varying(255) NOT NULL,
                "original_filename" character varying(255) NOT NULL,
                "hash" character varying(255) NOT NULL,
                "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                "delimiter" character,
                "quote" character,
                "linebreak" character varying,
                "revision_id" uuid,
                CONSTRAINT "PK_fact_table_id" PRIMARY KEY ("id")
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
            CREATE TABLE "category_info" (
                "category" text NOT NULL,
                "lang" text NOT NULL,
                "description" text NOT NULL,
                "notes" text,
                CONSTRAINT "PK_b352b1990fc76e4cb4c7c9e0c9d" PRIMARY KEY ("category", "lang")
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
            ALTER TABLE "category_key"
            ADD CONSTRAINT "FK_087b36846d67092609821a62756" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info"
            ADD CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data"
            ADD CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
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
            ALTER TABLE "dataset_info"
            ADD CONSTRAINT "FK_dataset_info_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension_info"
            ADD CONSTRAINT "FK_dimension_info_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_info"
            ADD CONSTRAINT "FK_measure_info_measure_id" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "measure"
            ADD CONSTRAINT "FK_measure_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "measure"
            ADD CONSTRAINT "FK_measure_lookup_table_id_lookup_table_measure_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "provider_source"
            ADD CONSTRAINT "FK_provider_source_provider_id" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id", "language") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id", "language") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id", "language") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic"
            ADD CONSTRAINT "FK_dataset_topic_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic"
            ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset"
            ADD CONSTRAINT "FK_dataset_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "fact_table_info"
            ADD CONSTRAINT "FK_fact_table_info_fact_table_id" FOREIGN KEY ("fact_table_id") REFERENCES "fact_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "fact_table"
            ADD CONSTRAINT "FK_fact_table_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION
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
            ALTER TABLE "category_info"
            ADD CONSTRAINT "FK_68028565126809c1e925e6f9334" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data_info"
            ADD CONSTRAINT "FK_f671fde9c769286ba971485ba09" FOREIGN KEY ("item_id", "version_no", "category_key") REFERENCES "reference_data"("item_id", "version_no", "category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "reference_data_info" DROP CONSTRAINT "FK_f671fde9c769286ba971485ba09"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_info" DROP CONSTRAINT "FK_68028565126809c1e925e6f9334"
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
            ALTER TABLE "fact_table" DROP CONSTRAINT "FK_fact_table_revision_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "fact_table_info" DROP CONSTRAINT "FK_fact_table_info_fact_table_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_created_by"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_source_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_provider_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider" DROP CONSTRAINT "FK_dataset_provider_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "provider_source" DROP CONSTRAINT "FK_provider_source_provider_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_lookup_table_id_lookup_table_measure_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_info" DROP CONSTRAINT "FK_measure_info_measure_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension_info" DROP CONSTRAINT "FK_dimension_info_dimension_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_info" DROP CONSTRAINT "FK_dataset_info_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy" DROP CONSTRAINT "FK_6ca6a866371cdd67a638df8a74c"
        `);
        await queryRunner.query(`
            ALTER TABLE "hierarchy" DROP CONSTRAINT "FK_aaba494e02eaa91111d549e5763"
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data" DROP CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key" DROP CONSTRAINT "FK_087b36846d67092609821a62756"
        `);
        await queryRunner.query(`
            DROP TABLE "reference_data_info"
        `);
        await queryRunner.query(`
            DROP TABLE "category_info"
        `);
        await queryRunner.query(`
            DROP TABLE "revision"
        `);
        await queryRunner.query(`
            DROP TABLE "fact_table"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."fact_table_filetype_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "fact_table_info"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."fact_table_info_column_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset_topic"
        `);
        await queryRunner.query(`
            DROP TABLE "topic"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset_provider"
        `);
        await queryRunner.query(`
            DROP TABLE "provider"
        `);
        await queryRunner.query(`
            DROP TABLE "provider_source"
        `);
        await queryRunner.query(`
            DROP TABLE "dimension"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."dimension_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "lookup_table"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."lookup_table_filetype_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "measure"
        `);
        await queryRunner.query(`
            DROP TABLE "measure_info"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_info_display_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "dimension_info"
        `);
        await queryRunner.query(`
            DROP TABLE "dataset_info"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."dataset_info_designation_enum"
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
            DROP TABLE "hierarchy"
        `);
        await queryRunner.query(`
            DROP TABLE "reference_data"
        `);
        await queryRunner.query(`
            DROP TABLE "category_key_info"
        `);
        await queryRunner.query(`
            DROP TABLE "category_key"
        `);
        await queryRunner.query(`
            DROP TABLE "category"
        `);
    }
}
