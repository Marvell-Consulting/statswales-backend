import { MigrationInterface, QueryRunner } from 'typeorm';

export class Tasks1745942366601 implements MigrationInterface {
  name = 'Tasks1745942366601';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE "task" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "action" text NOT NULL, "status" text NOT NULL, "entity" text, "entity_id" text, "comment" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "submitted_by" uuid, "response_by" uuid, CONSTRAINT "PK_task_id" PRIMARY KEY ("id"))`
    );
    await queryRunner.query(`CREATE INDEX "IDX_task_status" ON "task" ("status") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_entity_entity_id" ON "task" ("entity") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_submitted_by" ON "task" ("submitted_by") `);
    await queryRunner.query(`CREATE INDEX "IDX_task_response_by" ON "task" ("response_by") `);
    await queryRunner.query(
      `ALTER TABLE "task" ADD CONSTRAINT "FK_task_submitted_by" FOREIGN KEY ("submitted_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
    await queryRunner.query(
      `ALTER TABLE "task" ADD CONSTRAINT "FK_task_response_by" FOREIGN KEY ("response_by") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_task_response_by"`);
    await queryRunner.query(`ALTER TABLE "task" DROP CONSTRAINT "FK_task_submitted_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_response_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_submitted_by"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_entity_entity_id"`);
    await queryRunner.query(`DROP INDEX "public"."IDX_task_status"`);
    await queryRunner.query(`DROP TABLE "task"`);
  }
}
