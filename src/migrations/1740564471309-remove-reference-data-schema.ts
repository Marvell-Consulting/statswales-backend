import { MigrationInterface, QueryRunner } from 'typeorm';

export class RemoveReferenceDataSchema1740564471309 implements MigrationInterface {
    name = 'RemoveReferenceDataSchema1740564471309';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_key_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "hierarchy"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "reference_data_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "reference_data"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_key"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category"
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_key_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "hierarchy"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "reference_data_info"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "reference_data"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category_key"
        `);
        await queryRunner.query(`
            DROP TABLE IF EXISTS "category"
        `);
    }
}
