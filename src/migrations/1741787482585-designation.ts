import { MigrationInterface, QueryRunner } from 'typeorm';

export class Designation1741787482585 implements MigrationInterface {
  name = 'Designation1741787482585';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" ALTER COLUMN "designation" TYPE text`);
    await queryRunner.query(`DROP TYPE "public"."revision_designation_enum"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "public"."revision_designation_enum" AS ENUM('official', 'accredited', 'in_development', 'none')`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ALTER COLUMN "designation" TYPE revision_designation_enum USING "designation"::revision_designation_enum`
    );
  }
}
