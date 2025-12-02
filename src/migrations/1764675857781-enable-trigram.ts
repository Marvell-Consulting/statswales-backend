import { MigrationInterface, QueryRunner } from 'typeorm';

export class EnableTrigram1764675857781 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // The pg_trgm extension provides functions and operators for determining the similarity of text based on
    // trigram matching. Useful for searching by dataset title or for comparing similarity of dataset titles.
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm;`);
    await queryRunner.query(
      `CREATE INDEX IF NOT EXISTS IDX_title_trgm_gist ON revision_metadata USING GIST (title gist_trgm_ops);`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_title_trgm_gist;`);
    await queryRunner.query(`DROP EXTENSION IF EXISTS pg_trgm;`);
  }
}
