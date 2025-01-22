import { MigrationInterface, QueryRunner } from 'typeorm';

export class EventLog1737384932015 implements MigrationInterface {
    name = 'EventLog1737384932015';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "event_log" ("id" SERIAL NOT NULL, "action" text NOT NULL, "entity" text NOT NULL, "entity_id" text NOT NULL, "data" jsonb, "user_id" uuid, "client" text, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_event_log_id" PRIMARY KEY ("id"))`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "event_log"`);
    }
}
