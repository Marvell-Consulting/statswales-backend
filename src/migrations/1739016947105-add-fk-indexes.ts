import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddFkIndexes1739016947105 implements MigrationInterface {
    name = 'AddFkIndexes1739016947105';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE INDEX "IDX_revison_dataset_id" ON "revision" ("dataset_id") `);
        await queryRunner.query(
            `CREATE INDEX "IDX_revison_previous_revision_id" ON "revision" ("previous_revision_id") `
        );
        await queryRunner.query(`CREATE INDEX "IDX_revison_created_by" ON "revision" ("created_by") `);
        await queryRunner.query(`CREATE INDEX "IDX_revison_approved_by" ON "revision" ("approved_by") `);
        await queryRunner.query(`CREATE INDEX "IDX_dimension_dataset_id" ON "dimension" ("dataset_id") `);
        await queryRunner.query(
            `CREATE INDEX "IDX_provider_source_provider_id_language" ON "provider_source" ("provider_id", "language") `
        );
        await queryRunner.query(`CREATE INDEX "IDX_dataset_provider_dataset_id" ON "dataset_provider" ("dataset_id") `);
        await queryRunner.query(
            `CREATE INDEX "IDX_dataset_provider_provider_id_language" ON "dataset_provider" ("provider_id", "language") `
        );
        await queryRunner.query(
            `CREATE INDEX "IDX_dataset_provider_provider_source_id_language" ON "dataset_provider" ("provider_source_id", "language") `
        );
        await queryRunner.query(`CREATE INDEX "IDX_dataset_topic_dataset_id" ON "dataset_topic" ("dataset_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_dataset_topic_topic_id" ON "dataset_topic" ("topic_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_dataset_created_by" ON "dataset" ("created_by") `);
        await queryRunner.query(`CREATE INDEX "IDX_dataset_team_id" ON "dataset" ("team_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_team_organisation_id" ON "team" ("organisation_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_category_key_category" ON "category_key" ("category") `);
        await queryRunner.query(`CREATE INDEX "IDX_reference_data_category_key" ON "reference_data" ("category_key") `);
        await queryRunner.query(
            `CREATE INDEX "IDX_hierarchy_item_id_version_no_category_key" ON "hierarchy" ("item_id", "version_no", "category_key") `
        );
        await queryRunner.query(
            `CREATE INDEX "IDX_hierarchy_parent_id_parent_version_parent_category" ON "hierarchy" ("parent_id", "parent_version", "parent_category") `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP INDEX "public"."IDX_hierarchy_parent_id_parent_version_parent_category"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_hierarchy_item_id_version_no_category_key"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_reference_data_category_key"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_category_key_category"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_team_organisation_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_team_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_created_by"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_topic_topic_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_topic_dataset_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_provider_provider_source_id_language"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_provider_provider_id_language"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dataset_provider_dataset_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_provider_source_provider_id_language"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_dimension_dataset_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revison_approved_by"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revison_created_by"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revison_previous_revision_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_revison_dataset_id"`);
    }
}
