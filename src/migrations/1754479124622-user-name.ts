import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserName1754479124622 implements MigrationInterface {
  name = 'UserName1754479124622';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user" ADD "name" text`);

    await queryRunner.query(
      `UPDATE "user" SET "name" = COALESCE("given_name", '') || ' ' || COALESCE("family_name", '')
      WHERE "given_name" IS NOT NULL OR "family_name" IS NOT NULL`
    );

    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "given_name"`);
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "family_name"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "name"`);
    await queryRunner.query(`ALTER TABLE "user" ADD "given_name" text`);
    await queryRunner.query(`ALTER TABLE "user" ADD "family_name" text`);
  }
}
