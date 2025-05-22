import { MigrationInterface, QueryRunner } from 'typeorm';

export class EventUser1747925197003 implements MigrationInterface {
  name = 'EventUser1747925197003';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE "event_log" ADD CONSTRAINT "FK_event_log_user_id" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "event_log" DROP CONSTRAINT "FK_event_log_user_id"`);
  }
}
