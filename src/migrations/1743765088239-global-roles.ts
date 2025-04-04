import { MigrationInterface, QueryRunner } from 'typeorm';

export class GlobalRoles1743765088239 implements MigrationInterface {
  name = 'GlobalRoles1743765088239';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user" ADD "global_roles" jsonb NOT NULL DEFAULT '[]'`);
    await queryRunner.query(`ALTER TABLE "user_group_role" ALTER COLUMN "roles" SET DEFAULT '[]'`);
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_roles" ON "user_group_role" ("roles") `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_roles"`);
    await queryRunner.query(`ALTER TABLE "user_group_role" ALTER COLUMN "roles" DROP DEFAULT`);
    await queryRunner.query(`ALTER TABLE "user" DROP COLUMN "global_roles"`);
  }
}
