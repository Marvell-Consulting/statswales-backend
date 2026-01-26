import { MigrationInterface, QueryRunner } from 'typeorm';

export class Search1768913380304 implements MigrationInterface {
  name = 'Search1768913380304';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "search_log" (
        "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
        "mode" text NOT NULL,
        "keywords" text NOT NULL,
        "result_count" integer,
        "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT "PK_search_log_id" PRIMARY KEY ("id")
      )`
    );

    // full text search setup
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS unaccent;`);
    await queryRunner.query(
      `CREATE INDEX IF NOT EXISTS IDX_title_trgm_gist_ci ON revision_metadata USING GIN ((lower(title)) gin_trgm_ops);`
    );

    // Add tsvector columns (trigger will maintain them)
    await queryRunner.query(`
      ALTER TABLE revision_metadata
      ADD COLUMN IF NOT EXISTS fts tsvector,
      ADD COLUMN IF NOT EXISTS fts_simple tsvector;
    `);

    // Backfill existing rows
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
        );
    `);

    // Create trigger function to maintain fts columns on insert/update
    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION revision_metadata_update_fts() RETURNS trigger AS $$
      BEGIN
        -- fts: English config for en-GB, NULL for other languages
        NEW.fts := CASE
          WHEN NEW.language = 'en-GB' THEN (
            setweight(to_tsvector('english', unaccent(coalesce(NEW.title, ''))), 'A') ||
            setweight(to_tsvector('english', unaccent(coalesce(NEW.summary, ''))), 'B')
          )
          ELSE NULL
        END;

        -- fts_simple: simple config for all languages
        NEW.fts_simple := (
          setweight(to_tsvector('simple', unaccent(coalesce(NEW.title, ''))), 'A') ||
          setweight(to_tsvector('simple', unaccent(coalesce(NEW.summary, ''))), 'B')
        );

        RETURN NEW;
      END
      $$ LANGUAGE plpgsql;
    `);

    // Create trigger to call the function on insert/update
    await queryRunner.query(`
      DROP TRIGGER IF EXISTS revision_metadata_fts_trg ON revision_metadata;
      CREATE TRIGGER revision_metadata_fts_trg
      BEFORE INSERT OR UPDATE OF title, summary
      ON revision_metadata
      FOR EACH ROW
      EXECUTE FUNCTION revision_metadata_update_fts();
    `);

    // Index the tsvector columns
    await queryRunner.query(
      `CREATE INDEX IF NOT EXISTS IDX_revision_metadata_fts_gin ON revision_metadata USING GIN (fts);`
    );
    await queryRunner.query(
      `CREATE INDEX IF NOT EXISTS IDX_revision_metadata_fts_simple_gin ON revision_metadata USING GIN (fts_simple);`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_revision_metadata_fts_simple_gin;`);
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_revision_metadata_fts_gin;`);
    await queryRunner.query(`DROP TRIGGER IF EXISTS revision_metadata_fts_trg ON revision_metadata;`);
    await queryRunner.query(`DROP FUNCTION IF EXISTS revision_metadata_update_fts;`);
    await queryRunner.query(`ALTER TABLE revision_metadata DROP COLUMN IF EXISTS fts_simple;`);
    await queryRunner.query(`ALTER TABLE revision_metadata DROP COLUMN IF EXISTS fts;`);
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_title_trgm_gist_ci;`);
    await queryRunner.query(`DROP EXTENSION IF EXISTS unaccent;`);

    await queryRunner.query(`DROP TABLE "search_log"`);
  }
}
