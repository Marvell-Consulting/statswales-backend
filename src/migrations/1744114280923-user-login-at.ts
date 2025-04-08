import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserLoginAt1744114280923 implements MigrationInterface {
  name = 'UserLoginAt1744114280923';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user" ADD "last_login_at" TIMESTAMP WITH TIME ZONE`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "last_login_at"`);
  }
}
