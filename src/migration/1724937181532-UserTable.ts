import { MigrationInterface, QueryRunner } from 'typeorm';

export class UserTable1724937181532 implements MigrationInterface {
    name = 'UserTable1724937181532';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(
            `CREATE TABLE "user" (
                "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
                "provider" character varying NOT NULL,
                "provider_user_id" character varying NOT NULL,
                "email" character varying NOT NULL,
                "email_verified" boolean NOT NULL DEFAULT false,
                "first_name" character varying,
                "last_name" character varying,
                "created_at" TIME WITH TIME ZONE NOT NULL DEFAULT now(),
                "updated_at" TIME WITH TIME ZONE NOT NULL DEFAULT now(),
                CONSTRAINT "PK_user_id" PRIMARY KEY ("id")
            )`
        );
        await queryRunner.query(`CREATE INDEX "IX_user_provider" ON "user" ("provider") `);
        await queryRunner.query(`CREATE UNIQUE INDEX "UX_user_email" ON "user" ("email") `);
        await queryRunner.query(
            `CREATE UNIQUE INDEX "UX_user_provider_provider_user_id" ON "user" ("provider", "provider_user_id") `
        );
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP INDEX "public"."UX_user_provider_provider_user_id"`);
        await queryRunner.query(`DROP INDEX "public"."UX_user_email"`);
        await queryRunner.query(`DROP INDEX "public"."IX_user_provider"`);
        await queryRunner.query(`DROP TABLE "user"`);
    }
}
