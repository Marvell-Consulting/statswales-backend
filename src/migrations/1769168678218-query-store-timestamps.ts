import { MigrationInterface, QueryRunner } from 'typeorm';

export class QueryStoreTimestamps1769168678218 implements MigrationInterface {
  name = 'QueryStoreTimestamps1769168678218';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "query_store" ADD "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now()`);
    await queryRunner.query(`ALTER TABLE "query_store" ADD "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now()`);
    // Backfill any NULL timestamps with current time
    await queryRunner.query(`UPDATE "query_store" SET "created_at" = now() WHERE "created_at" IS NULL`);
    await queryRunner.query(`UPDATE "query_store" SET "updated_at" = now() WHERE "updated_at" IS NULL`);
    // Make columns NOT NULL
    await queryRunner.query(`ALTER TABLE "query_store" ALTER COLUMN "created_at" SET NOT NULL`);
    await queryRunner.query(`ALTER TABLE "query_store" ALTER COLUMN "updated_at" SET NOT NULL`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "query_store" DROP COLUMN "updated_at"`);
    await queryRunner.query(`ALTER TABLE "query_store" DROP COLUMN "created_at"`);
  }
}
