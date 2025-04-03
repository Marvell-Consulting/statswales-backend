import { MigrationInterface, QueryRunner } from 'typeorm';

export class OriginalFileOnLookup1743591867626 implements MigrationInterface {
  name = 'OriginalFileOnLookup1743591867626';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "lookup_table"
            ADD "original_filename" character varying(255)
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TABLE "lookup_table" DROP COLUMN "original_filename"
        `);
  }
}
