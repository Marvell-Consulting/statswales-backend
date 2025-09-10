import { MigrationInterface, QueryRunner } from 'typeorm';

export class UnpublishArchive1757407828605 implements MigrationInterface {
  name = 'UnpublishArchive1757407828605';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "dataset" RENAME COLUMN "live" TO "first_published_at"`);
    await queryRunner.query(`ALTER TABLE "dataset" RENAME COLUMN "archive" TO "archived_at"`);
    await queryRunner.query(`ALTER TABLE "dataset" ADD COLUMN "unpublished_at" TIMESTAMP WITH TIME ZONE`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "unpublished_at"`);
    await queryRunner.query(`ALTER TABLE "dataset" RENAME COLUMN "archived_at" TO "archive"`);
    await queryRunner.query(`ALTER TABLE "dataset" RENAME COLUMN "first_published_at" TO "live"`);
  }
}
