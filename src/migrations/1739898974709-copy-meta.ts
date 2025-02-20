import { MigrationInterface, QueryRunner } from 'typeorm';

/* This migration does not alter the schema, it just copies existing data from dataset_metadata to revision_metadata */
export class CopyMeta1739898974709 implements MigrationInterface {
    name = 'CopyMeta1739898974709';

    public async up(queryRunner: QueryRunner): Promise<void> {
        // copy language independent metadata into revision
        await queryRunner.query(
            `
                UPDATE revision
                SET rounding_applied = dataset_metadata.rounding_applied,
                    update_frequency = dataset_metadata.update_frequency,
                    designation = dataset_metadata.designation::text::revision_designation_enum,
                    related_links = dataset_metadata.related_links
                FROM dataset_metadata
                WHERE revision.dataset_id = dataset_metadata.dataset_id
                AND dataset_metadata.language = 'en-GB'
            `
        );

        // copy language-based metadata into revision_metadata
        await queryRunner.query(
            `
                INSERT INTO revision_metadata (revision_id, "language", title, summary, collection, quality, rounding_description, created_at, updated_at)
                SELECT revision.id, dm.language, dm.title, dm.description, dm.collection, dm.quality, dm.rounding_description, dm.created_at, dm.updated_at
                FROM dataset_metadata dm
                INNER JOIN revision ON dm.dataset_id = revision.dataset_id
                ON CONFLICT (revision_id, language) DO NOTHING
            `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {}
}
