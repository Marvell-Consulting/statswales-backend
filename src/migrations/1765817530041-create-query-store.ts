import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateQueryStore1765817530041 implements MigrationInterface {
  name = 'CreateQueryStore1765817530041';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "query_store" (
      "id" text NOT NULL,
      "hash" text NOT NULL,
      "dataset_id" uuid NOT NULL,
      "revision_id" uuid NOT NULL,
      "request_object" jsonb NOT NULL,
      "query" jsonb NOT NULL,
      "total_lines" integer NOT NULL,
      "column_mapping" jsonb NOT NULL,
      CONSTRAINT "PK_query_store_id" PRIMARY KEY ("id")
      )`
    );
    await queryRunner.query(`CREATE INDEX "IDX_query_store_hash" ON "query_store" ("hash") `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX "public"."IDX_query_store_hash"`);
    await queryRunner.query(`DROP TABLE "query_store"`);
  }
}
