import { MigrationInterface, QueryRunner } from 'typeorm';

export class LowercaseUsers1745923823063 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    // delete deactivated users who have never logged in
    await queryRunner.query(`DELETE FROM "user" WHERE "status" = 'inactive' AND "last_login_at" IS NULL`);

    // lowercase all emails
    await queryRunner.query(`UPDATE "user" SET "email" = LOWER("email") WHERE "email" IS NOT NULL`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // nothing to do
  }
}
