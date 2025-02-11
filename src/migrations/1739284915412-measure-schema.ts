import { MigrationInterface, QueryRunner } from "typeorm";

export class MeasureSchema1739284915412 implements MigrationInterface {
    name = 'MeasureSchema1739284915412'

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "display_type"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_item_display_type_enum"
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."measure_item_format_enum" AS ENUM(
                'DECIMAL',
                'DOUBLE',
                'INTEGER',
                'BIGINT',
                'PERCENT',
                'VARCHAR',
                'BOOLEAN',
                'DATE',
                'DATETIME',
                'TIME',
                'TIMESTAMP'
            )
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "format" "public"."measure_item_format_enum" NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "decimal" integer
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "hierarchy" character varying
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "measure_type" character varying
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language", "reference")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "reference"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "reference" text NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language", "reference")
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "reference"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "reference" character varying NOT NULL
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_08558adffa34e2143696fbf6434" PRIMARY KEY ("reference", "measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP CONSTRAINT "PK_measure_item_measure_id_language"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD CONSTRAINT "PK_measure_item_measure_id_language" PRIMARY KEY ("measure_id", "language")
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "measure_type"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "hierarchy"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "decimal"
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item" DROP COLUMN "format"
        `);
        await queryRunner.query(`
            DROP TYPE "public"."measure_item_format_enum"
        `);
        await queryRunner.query(`
            CREATE TYPE "public"."measure_item_display_type_enum" AS ENUM(
                'DECIMAL',
                'DOUBLE',
                'INTEGER',
                'BIGINT',
                'PERCENT',
                'VARCHAR',
                'BOOLEAN',
                'DATE',
                'DATETIME',
                'TIME',
                'TIMESTAMP'
            )
        `);
        await queryRunner.query(`
            ALTER TABLE "measure_item"
            ADD "display_type" "public"."measure_item_display_type_enum" NOT NULL
        `);
    }

}
