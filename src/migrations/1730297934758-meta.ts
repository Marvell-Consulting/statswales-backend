import { MigrationInterface, QueryRunner } from 'typeorm';

export class Meta1730297934758 implements MigrationInterface {
    name = 'Meta1730297934758';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "collection" text`);
        await queryRunner.query(`ALTER TABLE "dataset_info" ADD "quality" text`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "quality"`);
        await queryRunner.query(`ALTER TABLE "dataset_info" DROP COLUMN "collection"`);
    }
}
