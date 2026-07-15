import { MigrationInterface, QueryRunner } from 'typeorm';

export class OneOpenPublishTaskPerDataset1784116138829 implements MigrationInterface {
  name = 'OneOpenPublishTaskPerDataset1784116138829';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Close orphaned open publish tasks whose revision has already been approved. These are the
    // tasks behind SW-1300: the revision is live but the (duplicate) task was never closed, so the
    // dataset falsely reports "update pending approval".
    await queryRunner.query(`
      UPDATE "task" t
      SET "open" = false, "status" = 'withdrawn'
      FROM "revision" r
      WHERE t."action" = 'publish'
        AND t."open" = true
        AND r."id" = (t."metadata"->>'revisionId')::uuid
        AND r."approved_at" IS NOT NULL
    `);

    // Deduplicate any remaining datasets that still have more than one open publish task, keeping
    // only the most recently created one so the unique index below can be created.
    await queryRunner.query(`
      UPDATE "task"
      SET "open" = false, "status" = 'withdrawn'
      WHERE "action" = 'publish'
        AND "open" = true
        AND "id" NOT IN (
          SELECT DISTINCT ON ("dataset_id") "id"
          FROM "task"
          WHERE "action" = 'publish' AND "open" = true
          ORDER BY "dataset_id", "created_at" DESC
        )
    `);

    // Backstop: guarantee at most one open publish task per dataset, closing the submit race that
    // allowed duplicates to be created in the first place.
    await queryRunner.query(`
      CREATE UNIQUE INDEX "UQ_task_one_open_publish_per_dataset"
      ON "task" ("dataset_id")
      WHERE "action" = 'publish' AND "open" = true
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP INDEX IF EXISTS "UQ_task_one_open_publish_per_dataset"`);
  }
}
