import { MigrationInterface, QueryRunner } from 'typeorm';

export class ChangeEnum1741278090812 implements MigrationInterface {
    name = 'ChangeEnum1741278090812';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TYPE "public"."dimension_type_enum"
            RENAME TO "dimension_type_enum_old"
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."dimension_type_enum" AS ENUM(
                'raw',
                'text',
                'numeric',
                'symbol',
                'lookup_table',
                'reference_data',
                'date_period',
                'date',
                'time_period',
                'time',
                'note_codes'
            )
        `);
        await queryRunner.query(`
            ALTER TABLE "dimension"
            ALTER COLUMN "type" TYPE "public"."dimension_type_enum" USING "type"::"text"::"public"."dimension_type_enum"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."dimension_type_enum_old"
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            DROP TYPE IF EXISTS "public"."dimension_type_enum_old"
        `);
    }
}
