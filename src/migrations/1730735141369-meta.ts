import { MigrationInterface, QueryRunner } from 'typeorm';

export class Meta1730735141369 implements MigrationInterface {
    name = 'Meta1730735141369';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "collection" text`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "quality" text`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "rounding_applied" boolean`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "rounding_description" text`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "related_links" jsonb`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "update_frequency" text`);
        await queryRunner.query(
            `CREATE TYPE "public"."dataset_info_designation_enum" AS ENUM('official', 'accredited', 'in_development', 'none')`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset_info" ADD "designation" "public"."dataset_info_designation_enum"`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "designation"`);
        await queryRunner.query(`DROP TYPE "public"."dataset_info_designation_enum"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "update_frequency"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "related_links"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "rounding_description"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "rounding_applied"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "quality"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "collection"`);
    }
}
