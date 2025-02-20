import { MigrationInterface, QueryRunner } from 'typeorm';

export class EnumCleanup1739898974509 implements MigrationInterface {
    name = 'EnumCleanup1739898974509';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `ALTER TYPE "public"."measure_rows_format_enum" RENAME TO "measure_rows_format_enum_old"`
        );
        await queryRunner.query(
            `CREATE TYPE "public"."measure_row_format_enum" AS ENUM('boolean', 'decimal', 'float', 'integer', 'long', 'percentage', 'string', 'text', 'date', 'datetime', 'time', 'timestamp')`
        );
        await queryRunner.query(
            `ALTER TABLE "measure_row" ALTER COLUMN "format" TYPE "public"."measure_row_format_enum" USING "format"::"text"::"public"."measure_row_format_enum"`
        );
        await queryRunner.query(`DROP TYPE "public"."measure_rows_format_enum_old"`);
        await queryRunner.query(
            `ALTER TYPE "public"."fact_table_columns_column_type_enum" RENAME TO "fact_table_columns_column_type_enum_old"`
        );
        await queryRunner.query(
            `CREATE TYPE "public"."fact_table_column_column_type_enum" AS ENUM('data_values', 'note_codes', 'dimension', 'measure', 'time', 'ignore', 'unknown', 'line_number')`
        );
        await queryRunner.query(
            `ALTER TABLE "fact_table_column" ALTER COLUMN "column_type" TYPE "public"."fact_table_column_column_type_enum" USING "column_type"::"text"::"public"."fact_table_column_column_type_enum"`
        );
        await queryRunner.query(`DROP TYPE "public"."fact_table_columns_column_type_enum_old"`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TYPE "public"."fact_table_columns_column_type_enum_old" AS ENUM('data_values', 'note_codes', 'dimension', 'measure', 'time', 'ignore', 'unknown', 'line_number')`
        );
        await queryRunner.query(
            `ALTER TABLE "fact_table_column" ALTER COLUMN "column_type" TYPE "public"."fact_table_columns_column_type_enum_old" USING "column_type"::"text"::"public"."fact_table_columns_column_type_enum_old"`
        );
        await queryRunner.query(`DROP TYPE "public"."fact_table_column_column_type_enum"`);
        await queryRunner.query(
            `ALTER TYPE "public"."fact_table_columns_column_type_enum_old" RENAME TO "fact_table_columns_column_type_enum"`
        );
        await queryRunner.query(
            `CREATE TYPE "public"."measure_rows_format_enum_old" AS ENUM('boolean', 'decimal', 'float', 'integer', 'long', 'percentage', 'string', 'text', 'date', 'datetime', 'time', 'timestamp')`
        );
        await queryRunner.query(
            `ALTER TABLE "measure_row" ALTER COLUMN "format" TYPE "public"."measure_rows_format_enum_old" USING "format"::"text"::"public"."measure_rows_format_enum_old"`
        );
        await queryRunner.query(`DROP TYPE "public"."measure_row_format_enum"`);
        await queryRunner.query(
            `ALTER TYPE "public"."measure_rows_format_enum_old" RENAME TO "measure_rows_format_enum"`
        );
    }
}
