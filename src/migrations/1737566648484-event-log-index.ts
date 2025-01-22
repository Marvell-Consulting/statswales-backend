import { MigrationInterface, QueryRunner } from 'typeorm';

export class EventLogIndex1737566648484 implements MigrationInterface {
    name = 'EventLogIndex1737566648484';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE INDEX "IDX_event_log_entity_id" ON "event_log" ("entity_id") `);
        await queryRunner.query(`CREATE INDEX "IDX_event_log_user_id" ON "event_log" ("user_id") `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP INDEX "public"."IDX_event_log_user_id"`);
        await queryRunner.query(`DROP INDEX "public"."IDX_event_log_entity_id"`);
    }
}
