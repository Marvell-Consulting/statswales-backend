import { MigrationInterface, QueryRunner } from 'typeorm';

export class OrgsAndTeams1733756465492 implements MigrationInterface {
    name = 'OrgsAndTeams1733756465492';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "organisation_info" ("organisation_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_organisation_info_organisation_id_language" PRIMARY KEY ("organisation_id", "language"))`
        );
        await queryRunner.query(
            `CREATE TABLE "organisation" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_organisation_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(
            `CREATE TABLE "team_info" ("team_id" uuid NOT NULL, "language" character varying(5) NOT NULL, "name" text, "email" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), CONSTRAINT "PK_team_info_team_id_language" PRIMARY KEY ("team_id", "language"))`
        );
        await queryRunner.query(
            `CREATE TABLE "team" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "prefix" text NOT NULL, "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), "organisation_id" uuid, CONSTRAINT "PK_team_id" PRIMARY KEY ("id"))`
        );
        await queryRunner.query(`ALTER TABLE "dataset" ADD "team_id" uuid`);
        await queryRunner.query(
            `ALTER TABLE "dataset" ADD CONSTRAINT "FK_dataset_team_id" FOREIGN KEY ("team_id") REFERENCES "team"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "organisation_info" ADD CONSTRAINT "FK_organisation_info_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "team_info" ADD CONSTRAINT "FK_team_info_team_id" FOREIGN KEY ("team_id") REFERENCES "team"("id") ON DELETE CASCADE ON UPDATE NO ACTION`
        );
        await queryRunner.query(
            `ALTER TABLE "team" ADD CONSTRAINT "FK_team_organisation_id" FOREIGN KEY ("organisation_id") REFERENCES "organisation"("id") ON DELETE NO ACTION ON UPDATE NO ACTION`
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`ALTER TABLE "team" DROP CONSTRAINT "FK_team_organisation_id"`);
        await queryRunner.query(`ALTER TABLE "team_info" DROP CONSTRAINT "FK_team_info_team_id"`);
        await queryRunner.query(
            `ALTER TABLE "organisation_info" DROP CONSTRAINT "FK_organisation_info_organisation_id"`
        );
        await queryRunner.query(`ALTER TABLE "dataset" DROP CONSTRAINT "FK_dataset_team_id"`);
        await queryRunner.query(`ALTER TABLE "dataset" DROP COLUMN "team_id"`);
        await queryRunner.query(`DROP TABLE "team"`);
        await queryRunner.query(`DROP TABLE "team_info"`);
        await queryRunner.query(`DROP TABLE "organisation"`);
        await queryRunner.query(`DROP TABLE "organisation_info"`);
    }
}
