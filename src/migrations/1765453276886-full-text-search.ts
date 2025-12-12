import { MigrationInterface, QueryRunner } from 'typeorm';

export class FullTextSearch1765453276886 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
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
            'simple'::regconfig,
            unaccent(coalesce(title, ''))
          ), 'A'
        ) ||
        setweight(
          to_tsvector(
            'simple'::regconfig,
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
              'simple'::regconfig,
              unaccent(coalesce(NEW.title, ''))
            ), 'A'
          ) ||
          setweight(
            to_tsvector(
              'simple'::regconfig,
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
  }
}
