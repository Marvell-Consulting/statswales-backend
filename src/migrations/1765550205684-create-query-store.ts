import { MigrationInterface, QueryRunner } from 'typeorm';

export class CreateQueryStore1765550205684 implements MigrationInterface {
  name = 'CreateQueryStore1765550205684';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "query-store" ("id" text NOT NULL, "hash" text NOT NULL, "dataset_id" uuid NOT NULL, "revision_id" uuid NOT NULL, "request_object" jsonb NOT NULL, "query" jsonb NOT NULL, "total_lines" integer NOT NULL, "column_mapping" jsonb NOT NULL, CONSTRAINT "PK_ff83b28f15f101bd8ed7347d23a" PRIMARY KEY ("id"))`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE "query-store"`);
  }
}
