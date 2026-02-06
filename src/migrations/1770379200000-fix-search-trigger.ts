import { MigrationInterface, QueryRunner } from 'typeorm';

export class FixSearchTrigger1770379200000 implements MigrationInterface {
  name = 'FixSearchTrigger1770379200000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Drop and recreate the trigger to include updated_at in the UPDATE OF clause as it changes on every write to the
    // revision metadata. Not sure why but the original trigger was not firing when revisions were created or updated,
    // possibly due to the way TypeORM handles update queries.
    await queryRunner.query(`
      DROP TRIGGER IF EXISTS revision_metadata_fts_trg ON revision_metadata;
      CREATE TRIGGER revision_metadata_fts_trg
      BEFORE INSERT OR UPDATE OF title, summary, updated_at
      ON revision_metadata
      FOR EACH ROW
      EXECUTE FUNCTION revision_metadata_update_fts();
    `);

    // Backfill any rows that have NULL fts_simple due to the trigger not firing
    await queryRunner.query(`
      UPDATE revision_metadata SET
        fts = CASE
          WHEN language = 'en-GB' THEN (
            setweight(to_tsvector('english', unaccent(coalesce(title, ''))), 'A') ||
            setweight(to_tsvector('english', unaccent(coalesce(summary, ''))), 'B')
          )
          ELSE NULL
        END,
        fts_simple = (
          setweight(to_tsvector('simple', unaccent(coalesce(title, ''))), 'A') ||
          setweight(to_tsvector('simple', unaccent(coalesce(summary, ''))), 'B')
        )
      WHERE fts_simple IS NULL;
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Revert to the original trigger definition
    await queryRunner.query(`
      DROP TRIGGER IF EXISTS revision_metadata_fts_trg ON revision_metadata;
      CREATE TRIGGER revision_metadata_fts_trg
      BEFORE INSERT OR UPDATE OF title, summary
      ON revision_metadata
      FOR EACH ROW
      EXECUTE FUNCTION revision_metadata_update_fts();
    `);
  }
}
