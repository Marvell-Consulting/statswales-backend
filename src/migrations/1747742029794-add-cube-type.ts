import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddCubeType1747742029794 implements MigrationInterface {
  name = 'AddCubeType1747742029794';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TYPE "public"."revision_cube_type_enum" AS ENUM('PostgresCube', 'PostgresProtoCube', 'DuckDBCube', 'DuckDBProtoCube')`
    );
    await queryRunner.query(`ALTER TABLE "revision" ADD "cube_type" "public"."revision_cube_type_enum"`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "cube_type"`);
    await queryRunner.query(`DROP TYPE "public"."revision_cube_type_enum"`);
  }
}
