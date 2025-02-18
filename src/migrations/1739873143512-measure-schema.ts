import { MigrationInterface, QueryRunner } from 'typeorm';

export class MeasureSchema1739873143512 implements MigrationInterface {
    name = 'MeasureSchema1739873143512';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            DROP TABLE fact_table;
            DROP TYPE "public"."fact_table_column_type_enum";
        `);
        await queryRunner.query(`
            DROP TABLE measure_item;
            DROP TYPE "public"."measure_item_display_type_enum";
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."measure_rows_format_enum" AS ENUM(
                'boolean',
                'decimal',
                'float',
                'integer',
                'long',
                'percentage',
                'string',
                'text',
                'date',
                'datetime',
                'time',
                'timestamp'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "measure_row" (
                "measure_id" uuid NOT NULL,
                "language" character varying(5) NOT NULL,
                "reference" text NOT NULL,
                "format" "public"."measure_rows_format_enum" NOT NULL,
                "decimal" integer,
                "description" character varying NOT NULL,
                "sort_order" integer,
                "notes" text,
                "hierarchy" character varying,
                "measure_type" character varying,
                CONSTRAINT "PK_measure_row_measure_id_language_reference" PRIMARY KEY ("measure_id", "language", "reference")
            )
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."fact_table_columns_column_type_enum" AS ENUM(
                'data_values',
                'note_codes',
                'dimension',
                'measure',
                'time',
                'ignore',
                'unknown',
                'line_number'
            )
        `);
        await queryRunner.query(`
            CREATE TABLE "fact_table_column" (
                "dataset_id" uuid NOT NULL,
                "column_name" character varying NOT NULL,
                "column_type" "public"."fact_table_columns_column_type_enum" NOT NULL,
                "column_datatype" character varying NOT NULL,
                "column_index" integer NOT NULL,
                CONSTRAINT "PK_fact_table_column_id_column_name" PRIMARY KEY ("dataset_id", "column_name")
            )
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_row"
            ADD CONSTRAINT "FK_measure_row_measure_id" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
        await queryRunner.query(`
            ALTER TABLE "fact_table_column"
            ADD CONSTRAINT "FK_dataset_id_fact_table_column_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE CASCADE ON UPDATE NO ACTION
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "fact_table_column" DROP CONSTRAINT "FK_dataset_id_fact_table_column_id"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_row" DROP CONSTRAINT "FK_measure_row_measure_id"
        `);
        await queryRunner.query(`
            DROP TABLE "fact_table_column"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."fact_table_columns_column_type_enum"
        `);
        await queryRunner.query(`
            DROP TABLE "measure_row"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_rows_format_enum"
        `);
    }
}
