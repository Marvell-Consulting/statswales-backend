import { MigrationInterface, QueryRunner } from 'typeorm';
import { quartersInYear } from 'date-fns/constants';

export class AddCubeType1747838732179 implements MigrationInterface {
  name = 'AddCubeType1747838732179';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE TYPE "public"."data_table_source_location_enum" AS ENUM('postgres', 'datalake')`);
    await queryRunner.query(
      `ALTER TABLE "data_table" ADD "source_location" "public"."data_table_source_location_enum" NOT NULL DEFAULT 'datalake'`
    );
    await queryRunner.query(
      `CREATE TYPE "public"."revision_cube_type_enum" AS ENUM('duckdb_proto_cube', 'duckdb_cube', 'postgres_proto_cube', 'postgres_cube')`
    );
    await queryRunner.query(
      `ALTER TABLE "revision" ADD "cube_type" "public"."revision_cube_type_enum" DEFAULT 'duckdb_cube'`
    );
    await queryRunner.createSchema('data_tables', true);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`ALTER TABLE "revision" DROP COLUMN "cube_type"`);
    await queryRunner.query(`DROP TYPE "public"."revision_cube_type_enum"`);
    await queryRunner.query(`ALTER TABLE "data_table" DROP COLUMN "source_location"`);
    await queryRunner.query(`DROP TYPE "public"."data_table_source_location_enum"`);
    await queryRunner.dropSchema('data_tables', true);
  }
}
