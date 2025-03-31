import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserGroupRoles1743180017172 implements MigrationInterface {
  name = 'UserGroupRoles1743180017172';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "user_group_role" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "roles" jsonb NOT NULL, "user_id" uuid NOT NULL, "group_id" uuid NOT NULL, CONSTRAINT "PK_user_group_role_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_user_id" ON "user_group_role" ("user_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_user_group_role_group_id" ON "user_group_role" ("group_id") `);
    await queryRunner.query(
      `ALTER TABLE "user_group_role" ADD CONSTRAINT "FK_user_group_role_user_id" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "user_group_role" ADD CONSTRAINT "FK_user_group_role_group_id" FOREIGN KEY ("group_id") REFERENCES "user_group"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
    );
    await queryRunner.query(`DROP INDEX IF EXISTS "IDX_a0f3c964602f06fed4c9e678dc"`);
    await queryRunner.query(`DROP INDEX IF EXISTS "IDX_17dd1f7e1090bba508cc84f09a"`);
    await queryRunner.query(`DROP TABLE IF EXISTS user_group_user`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "user_group_role" DROP CONSTRAINT "FK_user_group_role_group_id"`);
    await queryRunner.query(`ALTER TABLE "user_group_role" DROP CONSTRAINT "FK_user_group_role_user_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_group_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_user_group_role_user_id"`);
    await queryRunner.query(`DROP TABLE "user_group_role"`);
  }
}
