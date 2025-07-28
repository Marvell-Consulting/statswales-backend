import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddDataTableActionEnumCorrection1753708965595 implements MigrationInterface {
  name = 'AddDataTableActionEnumCorrection1753708965595';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            ALTER TYPE "public"."data_table_action_enum"
            RENAME TO "data_table_action_enum_old"
        `);
    await queryRunner.query(`
            CREATE TYPE "public"."data_table_action_enum" AS ENUM(
                'add',
                'replace_all',
                'revise',
                'add_revise',
                'correction'
            )
        `);
    await queryRunner.query(`
            ALTER TABLE "data_table"
            ALTER COLUMN "action" TYPE "public"."data_table_action_enum" USING "action"::"text"::"public"."data_table_action_enum"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."data_table_action_enum_old"
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            CREATE TYPE "public"."data_table_action_enum_old" AS ENUM('add', 'replace_all', 'revise', 'add_revise')
        `);
    await queryRunner.query(`
            ALTER TABLE "data_table"
            ALTER COLUMN "action" TYPE "public"."data_table_action_enum_old" USING "action"::"text"::"public"."data_table_action_enum_old"
        `);
    await queryRunner.query(`
            DROP TYPE "public"."data_table_action_enum"
        `);
    await queryRunner.query(`
            ALTER TYPE "public"."data_table_action_enum_old"
            RENAME TO "data_table_action_enum"
        `);
  }
}
