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

    // Add plain tsvector column (trigger will maintain it)
    await queryRunner.query(`
      ALTER TABLE revision_metadata
      ADD COLUMN IF NOT EXISTS fts tsvector;
    `);

    // Backfill existing rows
    await queryRunner.query(`
      UPDATE revision_metadata SET fts = (
        setweight(
          to_tsvector(
            CASE WHEN language = 'en-GB' THEN 'english'::regconfig ELSE 'simple'::regconfig END,
            unaccent(coalesce(title, ''))
          ), 'A'
        ) ||
        setweight(
          to_tsvector(
            CASE WHEN language = 'en-GB' THEN 'english'::regconfig ELSE 'simple'::regconfig END,
            unaccent(coalesce(summary, ''))
          ), 'B'
        )
      );
    `);

    // Create trigger function to maintain fts on insert/update
    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION revision_metadata_update_fts() RETURNS trigger AS $$
      BEGIN
        NEW.fts := (
          setweight(
            to_tsvector(
              CASE WHEN NEW.language = 'en-GB' THEN 'english'::regconfig ELSE 'simple'::regconfig END,
              unaccent(coalesce(NEW.title, ''))
            ), 'A'
          ) ||
          setweight(
            to_tsvector(
              CASE WHEN NEW.language = 'en-GB' THEN 'english'::regconfig ELSE 'simple'::regconfig END,
              unaccent(coalesce(NEW.summary, ''))
            ), 'B'
          )
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

    // Index the tsvector column
    await queryRunner.query(
      `CREATE INDEX IF NOT EXISTS IDX_revision_metadata_fts_gin ON revision_metadata USING GIN (fts);`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_revision_metadata_fts_gin;`);
    await queryRunner.query(`DROP TRIGGER IF EXISTS revision_metadata_fts_trg ON revision_metadata;`);
    await queryRunner.query(`DROP FUNCTION IF EXISTS revision_metadata_update_fts;`);
    await queryRunner.query(`ALTER TABLE revision_metadata DROP COLUMN IF EXISTS fts;`);
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_title_trgm_gist_ci;`);
    await queryRunner.query(`DROP EXTENSION IF EXISTS unaccent;`);

    await queryRunner.query(`DROP TABLE "search_log"`);
  }
}
