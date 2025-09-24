import { MigrationInterface, QueryRunner } from 'typeorm';

export class FixSources1758709762304 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // Replace any references to "Short term output indicators" - sw2_id 10319 with sw2_id 10171
    await queryRunner.query(`
      UPDATE revision_provider
      SET provider_source_id = '5d1f50a8-c132-4bc7-9344-508972c78aff'
      WHERE provider_source_id = 'e42fdda7-5fa1-4689-82f4-a52f0503f5ba'
    `);

    // Remove the duplicate source sw2_id 10319
    await queryRunner.query(`
      DELETE FROM provider_source
      WHERE id = 'e42fdda7-5fa1-4689-82f4-a52f0503f5ba'
    `);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async down(queryRunner: QueryRunner): Promise<void> {
    // do nothing
  }
}
