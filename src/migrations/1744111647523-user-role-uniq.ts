import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserRoleUniq1744111647523 implements MigrationInterface {
  name = 'UserRoleUniq1744111647523';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE UNIQUE INDEX "UX_user_group_role_user_id_group_id" ON "user_group_role" ("user_id", "group_id") `
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."UX_user_group_role_user_id_group_id"`);
  }
}
