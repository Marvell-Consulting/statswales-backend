import { MigrationInterface, QueryRunner } from 'typeorm';

export class RevShortcuts1739960952491 implements MigrationInterface {
    name = 'RevShortcuts1739960952491';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `ALTER TABLE "revision" ADD "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()`
        );
        await queryRunner.query(`ALTER TABLE "dataset" ADD "start_revision_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "UQ_ceb0e0e99af283c3b5d79ecac88" UNIQUE ("start_revision_id")`
        );
        await queryRunner.query(`ALTER TABLE "dataset" ADD "end_revision_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "UQ_548eada5e50d9535cacc68d0385" UNIQUE ("end_revision_id")`
        );
        await queryRunner.query(`ALTER TABLE "dataset" ADD "draft_revision_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "UQ_c23986c23ecf18770428fd36a37" UNIQUE ("draft_revision_id")`
        );
        await queryRunner.query(`ALTER TABLE "dataset" ADD "published_revision_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "UQ_56b4e7a3a89bbc70601e4edf3a5" UNIQUE ("published_revision_id")`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_start_revision_id" FOREIGN KEY ("start_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_end_revision_id" FOREIGN KEY ("end_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_draft_revision_id" FOREIGN KEY ("draft_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_published_revision_id" FOREIGN KEY ("published_revision_id") REFERENCES "revision"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_published_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_draft_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_end_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_start_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "UQ_56b4e7a3a89bbc70601e4edf3a5"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "published_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "UQ_c23986c23ecf18770428fd36a37"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "draft_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "UQ_548eada5e50d9535cacc68d0385"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "end_revision_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "UQ_ceb0e0e99af283c3b5d79ecac88"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "start_revision_id"`);
        await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "updated_at"`);
    }
}
