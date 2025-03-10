import { MigrationInterface, QueryRunner } from 'typeorm';

export class IntialSchema202503061741283385151 implements MigrationInterface {
  name = 'IntialSchema202503061741283385151';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "event_log" ("id" SERIAL NOT NULL, "action" text NOT NULL, "entity" text NOT NULL, "entity_id" text NOT NULL, "data" jsonb, "user_id" uuid, "client" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_event_log_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_event_log_entity_id" ON "event_log" ("entity_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_event_log_user_id" ON "event_log" ("user_id") `);
    await queryRunner.query(
      `CREATE TABLE "user" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "provider" character varying NOT NULL, "provider_user_id" character varying NOT NULL, "email" character varying NOT NULL, "email_verified" boolean NOT NULL DEFAULT false, "given_name" character varying, "family_name" character varying, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_user_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IX_user_provider" ON "user" ("provider") `);
    await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
    await queryRunner.query(
      `CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
    await queryRunner.query(
      `CREATE TABLE "data_table_description" ("fact_table_id" uuid NOT NULL, "column_name" character varying NOT NULL, "column_index" integer NOT NULL, "column_datatype" character varying NOT NULL, "fact_table_column" text, CONSTRAINT "PK_data_table_description_id_column_name" PRIMARY KEY ("fact_table_id", "column_name"))`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."data_table_filetype_enum" AS ENUM('csv', 'parquet', 'json', 'xlsx', 'csv.gz', 'json.gz', 'unknown')`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."data_table_action_enum" AS ENUM('add', 'replace_all', 'revise', 'add_revise')`
    );
    await queryRunner.query(
      `CREATE TABLE "data_table" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "mime_type" character varying(255) NOT NULL, "filetype" "public"."data_table_filetype_enum" NOT NULL, "filename" character varying(255) NOT NULL, "original_filename" character varying(255) NOT NULL, "hash" character varying(255) NOT NULL, "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "action" "public"."data_table_action_enum" NOT NULL, CONSTRAINT "PK_data_table_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TABLE "revision_metadata" ("revision_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "title" text, "summary" text, "collection" text, "quality" text, "rounding_description" text, "reason" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_revision_metadata_revision_id_language" PRIMARY KEY ("revision_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "provider_source" ("id" uuid NOT NULL, "language" character varying(5) NOT NULL, "sw2_id" integer, "name" text NOT NULL, "provider_id" uuid NOT NULL, CONSTRAINT "PK_provider_source_id_language" PRIMARY KEY ("id", "language"))`
    );
    await queryRunner.query(
      `CREATE INDEX "IDX_provider_source_provider_id_language" ON "provider_source" ("provider_id", "language") `
    );
    await queryRunner.query(
      `CREATE TABLE "provider" ("id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_provider_id_language" PRIMARY KEY ("id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "revision_provider" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "group_id" uuid NOT NULL, "revision_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "provider_id" uuid NOT NULL, "provider_source_id" uuid, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_revision_provider_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_revision_provider_revision_id" ON "revision_provider" ("revision_id") `);
    await queryRunner.query(
      `CREATE INDEX "IDX_revision_provider_provider_id_language" ON "revision_provider" ("provider_id", "language") `
    );
    await queryRunner.query(
      `CREATE INDEX "IDX_revision_provider_provider_source_id_language" ON "revision_provider" ("provider_source_id", "language") `
    );
    await queryRunner.query(
      `CREATE TABLE "topic" ("id" SERIAL NOT NULL, "path" text NOT NULL, "name_en" text, "name_cy" text, CONSTRAINT "PK_topic_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TABLE "revision_topic" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "revision_id" uuid NOT NULL, "topic_id" integer NOT NULL, CONSTRAINT "PK_revision_topic_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_revision_topic_revision_id" ON "revision_topic" ("revision_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_revision_topic_topic_id" ON "revision_topic" ("topic_id") `);
    await queryRunner.query(
      `CREATE TYPE "public"."revision_designation_enum" AS ENUM('official', 'accredited', 'in_development', 'none')`
    );
    await queryRunner.query(
      `CREATE TABLE "revision" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "dataset_id" uuid NOT NULL, "revision_index" integer NOT NULL, "previous_revision_id" uuid, "online_cube_filename" character varying(255), "data_table_id" uuid, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "approved_at" TIMESTAMP WITH TIME ZONE, "publish_at" TIMESTAMP WITH TIME ZONE, "tasks" jsonb, "rounding_applied" boolean, "update_frequency" text, "designation" "public"."revision_designation_enum", "related_links" jsonb, "created_by" uuid, "approved_by" uuid, CONSTRAINT "REL_4e80a15cf1f7e78d47396d6375" UNIQUE ("data_table_id"), CONSTRAINT "PK_revision_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_revison_dataset_id" ON "revision" ("dataset_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_revison_previous_revision_id" ON "revision" ("previous_revision_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_revison_data_table_id" ON "revision" ("data_table_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_revison_created_by" ON "revision" ("created_by") `);
    await queryRunner.query(`CREATE INDEX "IDX_revison_approved_by" ON "revision" ("approved_by") `);
    await queryRunner.query(
      `CREATE TABLE "dimension_metadata" ("dimension_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "description" text, "notes" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_dimension_metadata_dimension_id_language" PRIMARY KEY ("dimension_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."measure_row_format_enum" AS ENUM('boolean', 'decimal', 'float', 'integer', 'long', 'percentage', 'string', 'text', 'date', 'datetime', 'time', 'timestamp')`
    );
    await queryRunner.query(
      `CREATE TABLE "measure_row" ("measure_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "reference" text NOT NULL, "format" "public"."measure_row_format_enum" NOT NULL, "decimal" integer, "description" character varying NOT NULL, "sort_order" integer, "notes" text, "hierarchy" character varying, "measure_type" character varying, CONSTRAINT "PK_measure_row_measure_id_language_reference" PRIMARY KEY ("measure_id", "language", "reference"))`
    );
    await queryRunner.query(
      `CREATE TABLE "measure_metadata" ("measure_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "description" text, "notes" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_measure_metadata_measure_id_language" PRIMARY KEY ("measure_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "measure" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "fact_table_column" character varying NOT NULL, "join_column" character varying, "extractor" jsonb, "dataset_id" uuid, "lookup_table_id" uuid, CONSTRAINT "REL_d587d9925390a0bddf29598e9b" UNIQUE ("dataset_id"), CONSTRAINT "REL_08275fce5c8ccabbe8b82e2197" UNIQUE ("lookup_table_id"), CONSTRAINT "PK_measure_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."lookup_table_filetype_enum" AS ENUM('csv', 'parquet', 'json', 'xlsx', 'csv.gz', 'json.gz', 'unknown')`
    );
    await queryRunner.query(
      `CREATE TABLE "lookup_table" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "mime_type" character varying(255) NOT NULL, "filetype" "public"."lookup_table_filetype_enum" NOT NULL, "filename" character varying(255) NOT NULL, "hash" character varying(255) NOT NULL, "uploaded_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "is_statswales2_format" boolean NOT NULL, CONSTRAINT "PK_lookup_table_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."dimension_type_enum" AS ENUM('raw', 'text', 'numeric', 'symbol', 'lookup_table', 'reference_data', 'date_period', 'date', 'time_period', 'time', 'note_codes')`
    );
    await queryRunner.query(
      `CREATE TABLE "dimension" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "type" "public"."dimension_type_enum" NOT NULL, "extractor" jsonb, "join_column" character varying, "fact_table_column" character varying NOT NULL, "is_slice_dimension" boolean NOT NULL DEFAULT false, "dataset_id" uuid, "lookup_table_id" uuid, CONSTRAINT "REL_aa21260a923de02687ee91ef73" UNIQUE ("lookup_table_id"), CONSTRAINT "PK_dimension_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_dimension_dataset_id" ON "dimension" ("dataset_id") `);
    await queryRunner.query(
      `CREATE TYPE "public"."fact_table_column_column_type_enum" AS ENUM('data_values', 'note_codes', 'dimension', 'measure', 'time', 'ignore', 'unknown', 'line_number')`
    );
    await queryRunner.query(
      `CREATE TABLE "fact_table_column" ("dataset_id" uuid NOT NULL, "column_name" character varying NOT NULL, "column_type" "public"."fact_table_column_column_type_enum" NOT NULL, "column_datatype" character varying NOT NULL, "column_index" integer NOT NULL, CONSTRAINT "PK_fact_table_column_id_column_name" PRIMARY KEY ("dataset_id", "column_name"))`
    );
    await queryRunner.query(
      `CREATE TABLE "dataset" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "created_by" uuid NOT NULL, "live" TIMESTAMP WITH TIME ZONE, "archive" TIMESTAMP WITH TIME ZONE, "start_date" date, "end_date" date, "team_id" uuid, "start_revision_id" uuid, "end_revision_id" uuid, "draft_revision_id" uuid, "published_revision_id" uuid, CONSTRAINT "REL_ceb0e0e99af283c3b5d79ecac8" UNIQUE ("start_revision_id"), CONSTRAINT "REL_548eada5e50d9535cacc68d038" UNIQUE ("end_revision_id"), CONSTRAINT "REL_c23986c23ecf18770428fd36a3" UNIQUE ("draft_revision_id"), CONSTRAINT "REL_56b4e7a3a89bbc70601e4edf3a" UNIQUE ("published_revision_id"), CONSTRAINT "PK_dataset_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_dataset_created_by" ON "dataset" ("created_by") `);
    await queryRunner.query(`CREATE INDEX "IDX_dataset_team_id" ON "dataset" ("team_id") `);
    await queryRunner.query(
      `CREATE TABLE "organisation_info" ("organisation_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_organisation_info_organisation_id_language" PRIMARY KEY ("organisation_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "organisation" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_organisation_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(
      `CREATE TABLE "team_info" ("team_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text, "email" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_team_info_team_id_language" PRIMARY KEY ("team_id", "language"))`
    );
    await queryRunner.query(
      `CREATE TABLE "team" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "prefix" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "organisation_id" uuid, CONSTRAINT "PK_team_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_team_organisation_id" ON "team" ("organisation_id") `);
    await queryRunner.query(
      `ALTER TABLE "data_table_description" ADD CONSTRAINT "FK_data_table_description_fact_table_id" FOREIGN KEY ("fact_table_id") REFERENCES "data_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_metadata" ADD CONSTRAINT "FK_revision_metadata_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "provider_source" ADD CONSTRAINT "FK_provider_source_provider_id" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_provider" ADD CONSTRAINT "FK_revision_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id","language") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_topic" ADD CONSTRAINT "FK_revision_topic_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_topic" ADD CONSTRAINT "FK_revision_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_previous_revision_id" FOREIGN KEY ("previous_revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_data_table_id" FOREIGN KEY ("data_table_id") REFERENCES "data_table"("id") ON DELETE SET NULL ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD CONSTRAINT "FK_revision_approved_by" FOREIGN KEY ("approved_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dimension_metadata" ADD CONSTRAINT "FK_dimension_metadata_dimension_id" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "measure_row" ADD CONSTRAINT "FK_measure_row_measure_id" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "measure_metadata" ADD CONSTRAINT "FK_measure_metadata_measure_id" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "measure" ADD CONSTRAINT "FK_measure_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "measure" ADD CONSTRAINT "FK_measure_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "fact_table_column" ADD CONSTRAINT "FK_dataset_id_fact_table_column_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_start_revision_id" FOREIGN KEY ("start_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_end_revision_id" FOREIGN KEY ("end_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_draft_revision_id" FOREIGN KEY ("draft_revision_id") REFERENCES "revision"("id") ON DELETE SET NULL ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_published_revision_id" FOREIGN KEY ("published_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_team_id" FOREIGN KEY ("team_id") REFERENCES "team"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "organisation_info" ADD CONSTRAINT "FK_organisation_info_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "team_info" ADD CONSTRAINT "FK_team_info_team_id" FOREIGN KEY ("team_id") REFERENCES "team"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "team" ADD CONSTRAINT "FK_team_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "team" DROP CONSTRAINT "FK_team_organisation_id"`);
    await queryRunner.query(`ALTER TABLE "team_info" DROP CONSTRAINT "FK_team_info_team_id"`);
    await queryRunner.query(`ALTER TABLE "organisation_info" DROP CONSTRAINT "FK_organisation_info_organisation_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_team_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_published_revision_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_draft_revision_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_end_revision_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_start_revision_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_created_by"`);
    await queryRunner.query(`ALTER TABLE "fact_table_column" DROP CONSTRAINT "FK_dataset_id_fact_table_column_id"`);
    await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id"`);
    await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_dataset_id"`);
    await queryRunner.query(`ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_lookup_table_id"`);
    await queryRunner.query(`ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_dataset_id"`);
    await queryRunner.query(`ALTER TABLE "measure_metadata" DROP CONSTRAINT "FK_measure_metadata_measure_id"`);
    await queryRunner.query(`ALTER TABLE "measure_row" DROP CONSTRAINT "FK_measure_row_measure_id"`);
    await queryRunner.query(`ALTER TABLE "dimension_metadata" DROP CONSTRAINT "FK_dimension_metadata_dimension_id"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_approved_by"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_created_by"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_data_table_id"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_previous_revision_id"`);
    await queryRunner.query(`ALTER TABLE "revision" DROP CONSTRAINT "FK_revision_dataset_id"`);
    await queryRunner.query(`ALTER TABLE "revision_topic" DROP CONSTRAINT "FK_revision_topic_topic_id"`);
    await queryRunner.query(`ALTER TABLE "revision_topic" DROP CONSTRAINT "FK_revision_topic_revision_id"`);
    await queryRunner.query(
      `ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_provider_source_id_language"`
    );
    await queryRunner.query(
      `ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_provider_id_language"`
    );
    await queryRunner.query(`ALTER TABLE "revision_provider" DROP CONSTRAINT "FK_revision_provider_revision_id"`);
    await queryRunner.query(`ALTER TABLE "provider_source" DROP CONSTRAINT "FK_provider_source_provider_id"`);
    await queryRunner.query(`ALTER TABLE "revision_metadata" DROP CONSTRAINT "FK_revision_metadata_revision_id"`);
    await queryRunner.query(
      `ALTER TABLE "data_table_description" DROP CONSTRAINT "FK_data_table_description_fact_table_id"`
    );
    await queryRunner.query(`DROP INDEX "public"."IDX_team_organisation_id"`);
    await queryRunner.query(`DROP TABLE "team"`);
    await queryRunner.query(`DROP TABLE "team_info"`);
    await queryRunner.query(`DROP TABLE "organisation"`);
    await queryRunner.query(`DROP TABLE "organisation_info"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_dataset_team_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_dataset_created_by"`);
    await queryRunner.query(`DROP TABLE "dataset"`);
    await queryRunner.query(`DROP TABLE "fact_table_column"`);
    await queryRunner.query(`DROP TYPE "public"."fact_table_column_column_type_enum"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_dimension_dataset_id"`);
    await queryRunner.query(`DROP TABLE "dimension"`);
    await queryRunner.query(`DROP TYPE "public"."dimension_type_enum"`);
    await queryRunner.query(`DROP TABLE "lookup_table"`);
    await queryRunner.query(`DROP TYPE "public"."lookup_table_filetype_enum"`);
    await queryRunner.query(`DROP TABLE "measure"`);
    await queryRunner.query(`DROP TABLE "measure_metadata"`);
    await queryRunner.query(`DROP TABLE "measure_row"`);
    await queryRunner.query(`DROP TYPE "public"."measure_row_format_enum"`);
    await queryRunner.query(`DROP TABLE "dimension_metadata"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revison_approved_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revison_created_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revison_data_table_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revison_previous_revision_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revison_dataset_id"`);
    await queryRunner.query(`DROP TABLE "revision"`);
    await queryRunner.query(`DROP TYPE "public"."revision_designation_enum"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revision_topic_topic_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revision_topic_revision_id"`);
    await queryRunner.query(`DROP TABLE "revision_topic"`);
    await queryRunner.query(`DROP TABLE "topic"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_provider_source_id_language"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_provider_id_language"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_revision_provider_revision_id"`);
    await queryRunner.query(`DROP TABLE "revision_provider"`);
    await queryRunner.query(`DROP TABLE "provider"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_provider_source_provider_id_language"`);
    await queryRunner.query(`DROP TABLE "provider_source"`);
    await queryRunner.query(`DROP TABLE "revision_metadata"`);
    await queryRunner.query(`DROP TABLE "data_table"`);
    await queryRunner.query(`DROP TYPE "public"."data_table_action_enum"`);
    await queryRunner.query(`DROP TYPE "public"."data_table_filetype_enum"`);
    await queryRunner.query(`DROP TABLE "data_table_description"`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_provider_provider_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
    await queryRunner.query(`DROP INDEX "public"."IX_user_provider"`);
    await queryRunner.query(`DROP TABLE "user"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_event_log_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_event_log_entity_id"`);
    await queryRunner.query(`DROP TABLE "event_log"`);
  }
}
