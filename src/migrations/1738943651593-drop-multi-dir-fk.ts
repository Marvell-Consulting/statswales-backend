import { MigrationInterface, QueryRunner } from 'typeorm';

export class DropMultiDirFk1738943651593 implements MigrationInterface {
    name = 'DropMultiDirFk1738943651593';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_lookup_table_id_lookup_table_measure_id"`
        );
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP CONSTRAINT "FK_47ad3331d1237986c7a106f6ede"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP CONSTRAINT "FK_d897df215d38c8de48699f0bb1e"`);
        await queryRunner.query(
            `ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id"`
        );
        await queryRunner.query(`ALTER TABLE "category_key" DROP CONSTRAINT "FK_087b36846d67092609821a62756"`);
        await queryRunner.query(`ALTER TABLE "reference_data" DROP CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2"`);
        await queryRunner.query(`ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47"`);
        await queryRunner.query(`ALTER TABLE "category_info" DROP CONSTRAINT "FK_68028565126809c1e925e6f9334"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP CONSTRAINT "REL_d897df215d38c8de48699f0bb1"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP COLUMN "dimension_id"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP CONSTRAINT "REL_47ad3331d1237986c7a106f6ed"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" DROP COLUMN "measure_id"`);
        await queryRunner.query(
            `ALTER TABLE "measure" ADD CONSTRAINT "FK_measure_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_lookup_table_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "category_key" ADD CONSTRAINT "FK_category_key_category" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "reference_data" ADD CONSTRAINT "FK_reference_data_category_key" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "category_key_info" ADD CONSTRAINT "FK_category_key_info_category_key" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "category_info" ADD CONSTRAINT "FK_category_info_category" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "category_info" DROP CONSTRAINT "FK_category_info_category"`);
        await queryRunner.query(`ALTER TABLE "category_key_info" DROP CONSTRAINT "FK_category_key_info_category_key"`);
        await queryRunner.query(`ALTER TABLE "reference_data" DROP CONSTRAINT "FK_reference_data_category_key"`);
        await queryRunner.query(`ALTER TABLE "category_key" DROP CONSTRAINT "FK_category_key_category"`);
        await queryRunner.query(`ALTER TABLE "dimension" DROP CONSTRAINT "FK_dimension_lookup_table_id"`);
        await queryRunner.query(`ALTER TABLE "measure" DROP CONSTRAINT "FK_measure_lookup_table_id"`);
        await queryRunner.query(`ALTER TABLE "lookup_table" ADD "measure_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "lookup_table" ADD CONSTRAINT "REL_47ad3331d1237986c7a106f6ed" UNIQUE ("measure_id")`
        );
        await queryRunner.query(`ALTER TABLE "lookup_table" ADD "dimension_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "lookup_table" ADD CONSTRAINT "REL_d897df215d38c8de48699f0bb1" UNIQUE ("dimension_id")`
        );
        await queryRunner.query(
            `ALTER TABLE "category_info" ADD CONSTRAINT "FK_68028565126809c1e925e6f9334" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "category_key_info" ADD CONSTRAINT "FK_ec0b41bafd5605fff51fc0c8e47" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "reference_data" ADD CONSTRAINT "FK_dd4ff535904e339641b0b0d52c2" FOREIGN KEY ("category_key") REFERENCES "category_key"("category_key") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "category_key" ADD CONSTRAINT "FK_087b36846d67092609821a62756" FOREIGN KEY ("category") REFERENCES "category"("category") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "dimension" ADD CONSTRAINT "FK_dimension_lookup_table_id_lookup_table_dimension_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "lookup_table" ADD CONSTRAINT "FK_d897df215d38c8de48699f0bb1e" FOREIGN KEY ("dimension_id") REFERENCES "dimension"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "lookup_table" ADD CONSTRAINT "FK_47ad3331d1237986c7a106f6ede" FOREIGN KEY ("measure_id") REFERENCES "measure"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "measure" ADD CONSTRAINT "FK_measure_lookup_table_id_lookup_table_measure_id" FOREIGN KEY ("lookup_table_id") REFERENCES "lookup_table"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
    }
}
