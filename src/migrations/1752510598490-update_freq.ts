import { MigrationInterface, QueryRunner } from 'typeorm';

export class UpdateFreq1752510598490 implements MigrationInterface {
  name = 'UpdateFreq1752510598490';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "update_frequency"`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "update_frequency" json`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "update_frequency"`);
    await queryRunner.query(`ALTER TABLE "revision" ADD "update_frequency" text`);
  }
}
