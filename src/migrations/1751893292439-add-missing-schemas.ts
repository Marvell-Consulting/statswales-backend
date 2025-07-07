import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddMissingSchemas1751893292439 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createSchema('lookup_tables', true);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropSchema('lookup_tables', true, true);
  }
}
