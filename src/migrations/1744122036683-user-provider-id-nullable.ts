import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserProviderIdNullable1744122036683 implements MigrationInterface {
  name = 'UserProviderIdNullable1744122036683';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider_provider_user_id"`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" DROP NOT NULL`);
    await queryRunner.query(
      `CREATE INDEX "IDX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IDX_user_provider_provider_user_id"`);
    await queryRunner.query(`ALTER TABLE "user" ALTER COLUMN "provider_user_id" SET NOT NULL`);
    await queryRunner.query(
      `CREATE INDEX "IDX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
    );
  }
}
