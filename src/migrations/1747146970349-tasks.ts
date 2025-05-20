import { MigrationInterface, QueryRunner } from 'typeorm';

export class Tasks1747146970349 implements MigrationInterface {
  name = 'Tasks1747146970349';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "task" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "action" text NOT NULL, "status" text NOT NULL, "open" boolean NOT NULL DEFAULT true, "dataset_id" uuid, "metadata" jsonb, "comment" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "created_by" uuid, "updated_by" uuid, CONSTRAINT "PK_task_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_task_status" ON "task" ("status") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_open" ON "task" ("open") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_dataset_id" ON "task" ("dataset_id") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_created_by" ON "task" ("created_by") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_updated_by" ON "task" ("updated_by") `);
    await queryRunner.query(
      `ALTER TABLE "task" ADD CONSTRAINT "FK_task_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "task" ADD CONSTRAINT "FK_task_created_by" FOREIGN KEY ("created_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "task" ADD CONSTRAINT "FK_task_updated_by" FOREIGN KEY ("updated_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_task_updated_by"`);
    await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_task_created_by"`);
    await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_task_dataset_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_updated_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_created_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_dataset_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_open"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_status"`);
    await queryRunner.query(`DROP TABLE "task"`);
  }
}
