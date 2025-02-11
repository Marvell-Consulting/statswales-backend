import { MigrationInterface, QueryRunner } from "typeorm";

export class MeasureSchema1739282257427 implements MigrationInterface {
    name = 'MeasureSchema1739282257427'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "measure" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP CONSTRAINT "FK_47ad3331d1237986c7a106f6ede"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP CONSTRAINT "FK_d897df215d38c8de48699f0bb1e"
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id"
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
            ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key" DROP CONSTRAINT "FK_087b36846d67092609821a62756"
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data" DROP CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_info" DROP CONSTRAINT "FK_68028565126809c1e925e6f9334"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "display_type"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_item_display_type_enum"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP CONSTRAINT "REL_d897df215d38c8de48699f0bb1"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP COLUMN "dimension_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP CONSTRAINT "REL_47ad3331d1237986c7a106f6ed"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP COLUMN "measure_id"
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."measure_item_format_enum" AS ENUM(
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
            ALTER TABLE "measure_item"
            ADD "format" "public"."measure_item_format_enum" NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "decimal" integer
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "hierarchy" character varying
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "measure_type" character varying
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language", "reference")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_08558adffa34e2143696fbf6434"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "reference"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "reference" text NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language", "reference")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_revison_dataset_id" ON "revision" ("dataset_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_revison_previous_revision_id" ON "revision" ("previous_revision_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_revison_created_by" ON "revision" ("created_by")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_revison_approved_by" ON "revision" ("approved_by")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dimension_dataset_id" ON "dimension" ("dataset_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_provider_source_provider_id_language" ON "provider_source" ("provider_id", "language")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_provider_dataset_id" ON "dataset_provider" ("dataset_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_provider_provider_id_language" ON "dataset_provider" ("provider_id", "language")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_provider_provider_source_id_language" ON "dataset_provider" ("provider_source_id", "language")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_topic_dataset_id" ON "dataset_topic" ("dataset_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_topic_topic_id" ON "dataset_topic" ("topic_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_created_by" ON "dataset" ("created_by")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_dataset_team_id" ON "dataset" ("team_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_team_organisation_id" ON "team" ("organisation_id")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_category_key_category" ON "category_key" ("category")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_reference_data_category_key" ON "reference_data" ("category_key")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_hierarchy_item_id_version_no_category_key" ON "hierarchy" ("item_id", "version_no", "category_key")
        `);
        await queryRunner.query(`
            CREATE INDEX "IDX_hierarchy_parent_id_parent_version_parent_category" ON "hierarchy" ("parent_id", "parent_version", "parent_category")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure"
            ADD CONSTRAINT "FK_measure_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_provider_id_language" FOREIGN KEY ("provider_id", "language") REFERENCES "provider"("id", "language") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_provider"
            ADD CONSTRAINT "FK_dataset_provider_provider_source_id_language" FOREIGN KEY ("provider_source_id", "language") REFERENCES "provider_source"("id", "language") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic"
            ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key"
            ADD CONSTRAINT "FK_category_key_category" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data"
            ADD CONSTRAINT "FK_reference_data_category_key" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info"
            ADD CONSTRAINT "FK_category_key_info_category_key" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "category_info"
            ADD CONSTRAINT "FK_category_info_category" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "category_info" DROP CONSTRAINT "FK_category_info_category"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_category_key_info_category_key"
        `);
        await queryRunner.query(`
            ALTER TABLE "reference_data" DROP CONSTRAINT "FK_reference_data_category_key"
        `);
        await queryRunner.query(`
            ALTER TABLE "category_key" DROP CONSTRAINT "FK_category_key_category"
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic" DROP CONSTRAINT "FK_dataset_topic_topic_id"
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
            ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_lookup_table_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_hierarchy_parent_id_parent_version_parent_category"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_hierarchy_item_id_version_no_category_key"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_reference_data_category_key"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_category_key_category"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_team_organisation_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_team_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_created_by"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_topic_topic_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_topic_dataset_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_provider_provider_source_id_language"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_provider_provider_id_language"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dataset_provider_dataset_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_provider_source_provider_id_language"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_dimension_dataset_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_revison_approved_by"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_revison_created_by"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_revison_previous_revision_id"
        `);
        await queryRunner.query(`
            DROP INDEX "public"."IDX_revison_dataset_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "reference"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "reference" character varying NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_08558adffa34e2143696fbf6434" PRIMARY KEY ("measure_id", "language", "reference")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "measure_type"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "hierarchy"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "decimal"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "format"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_item_format_enum"
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD "measure_id" uuid
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD CONSTRAINT "REL_47ad3331d1237986c7a106f6ed" UNIQUE ("measure_id")
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD "dimension_id" uuid
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD CONSTRAINT "REL_d897df215d38c8de48699f0bb1" UNIQUE ("dimension_id")
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."measure_item_display_type_enum" AS ENUM(
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
            ALTER TABLE "measure_item"
            ADD "display_type" "public"."measure_item_display_type_enum" NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "category_info"
            ADD CONSTRAINT "FK_68028565126809c1e925e6f9334" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
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
            ALTER TABLE "category_key"
            ADD CONSTRAINT "FK_087b36846d67092609821a62756" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "dataset_topic"
            ADD CONSTRAINT "FK_dataset_topic_topic_id" FOREIGN KEY ("topic_id") REFERENCES "topic"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
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
            ALTER TABLE "dimension"
            ADD CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD CONSTRAINT "FK_d897df215d38c8de48699f0bb1e" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD CONSTRAINT "FK_47ad3331d1237986c7a106f6ede" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE NO ACTION ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "measure"
            ADD CONSTRAINT "FK_measure_lookup_table_id_lookup_table_measure_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
    }

}
