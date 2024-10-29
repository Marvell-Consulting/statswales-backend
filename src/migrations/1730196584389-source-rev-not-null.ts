import { MigrationInterface, QueryRunner } from 'typeorm';

export class Extras1730196584389 implements MigrationInterface {
    name = 'Extras1730196584389';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "source" DROP CONSTRAINT "FK_source_revision_id"`);
        await queryRunner.query(`ALTER TABLE "source" ALTER COLUMN "revision_id" SET NOT NULL`);
        await queryRunner.query(
            `ALTER TABLE "source" ADD CONSTRAINT "FK_source_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "source" DROP CONSTRAINT "FK_source_revision_id"`);
        await queryRunner.query(`ALTER TABLE "source" ALTER COLUMN "revision_id" DROP NOT NULL`);
        await queryRunner.query(
            `ALTER TABLE "source" ADD CONSTRAINT "FK_source_revision_id" FOREIGN KEY ("revision_id") REFERENCES "revision"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }
}
