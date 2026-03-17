import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddReplacementDataset1773656902813 implements MigrationInterface {
  name = 'AddReplacementDataset1773656902813';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "dataset" ADD "replacement_dataset_id" uuid`);
    await queryRunner.query(`ALTER TABLE "dataset" ADD "replacement_auto_redirect" boolean NOT NULL DEFAULT false`);
    await queryRunner.query(
      `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_replacement_dataset_id" FOREIGN KEY ("replacement_dataset_id") REFERENCES "dataset"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_replacement_dataset_id"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "replacement_auto_redirect"`);
    await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "replacement_dataset_id"`);
  }
}
