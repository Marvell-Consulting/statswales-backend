import { MigrationInterface, QueryRunner } from 'typeorm';

export class Migration1723729297617 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
                    CREATE TABLE users (
                        id UUID PRIMARY KEY,
                        username VARCHAR(255) NOT NULL UNIQUE,
                        email VARCHAR(255) NOT NULL UNIQUE,
                        oidc_subject VARCHAR(255) UNIQUE,
                        oidc_issuer VARCHAR(255),
                        access_token TEXT,
                        refresh_token TEXT,
                        id_token TEXT,
                        token_expiry TIMESTAMP,
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        profile_picture VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        active BOOLEAN NOT NULL DEFAULT true
                    );

                    CREATE TABLE dataset (
                        id UUID PRIMARY KEY,
                        creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by UUID,
                        live TIMESTAMP,
                        archive TIMESTAMP,
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    );

                    CREATE TABLE dataset_info (
                        id UUID PRIMARY KEY,
                        dataset_id UUID,
                        language VARCHAR(5),
                        title TEXT,
                        description TEXT,
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE
                    );

                    CREATE TABLE dimension_info (
                        id UUID PRIMARY KEY,
                        dimension_id UUID,
                        language VARCHAR(5),
                        name TEXT,
                        description TEXT,
                        notes TEXT,
                        FOREIGN KEY (dimension_id) REFERENCES dimension(id) ON DELETE CASCADE
                    );

                    CREATE TABLE revision (
                        id UUID PRIMARY KEY,
                        revision_index INT,
                        dataset_id UUID,
                        creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        previous_revision_id UUID,
                        online_cube_filename VARCHAR(255),
                        publish_date TIMESTAMP,
                        approval_date TIMESTAMP,
                        approved_by UUID,
                        created_by UUID,
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE,
                        FOREIGN KEY (previous_revision_id) REFERENCES revision(id) ON DELETE SET NULL,
                        FOREIGN KEY (approved_by) REFERENCES users(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    );

                    CREATE TABLE dimension (
                        id UUID PRIMARY KEY,
                        dataset_id UUID,
                        type VARCHAR(255) NOT NULL,
                        start_revision_id UUID NOT NULL,
                        finish_revision_id UUID,
                        validator TEXT,
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE,
                        FOREIGN KEY (start_revision_id) REFERENCES revision(id) ON DELETE CASCADE,
                        FOREIGN KEY (finish_revision_id) REFERENCES revision(id) ON DELETE SET NULL
                    );

                    CREATE TABLE csv_info (
                        import_id UUID PRIMARY KEY,
                        delimiter CHAR(1),
                        quote CHAR(1),
                        linebreak VARCHAR(2),
                        FOREIGN KEY (import_id) REFERENCES import(id) ON DELETE CASCADE
                    );

                    CREATE TABLE import (
                        id UUID PRIMARY KEY,
                        revision_id UUID,
                        csv_info UUID UNIQUE,
                        mime_type VARCHAR(255),
                        filename VARCHAR(255),
                        FOREIGN KEY (revision_id) REFERENCES revision(id) ON DELETE CASCADE,
                        FOREIGN KEY (csv_info) REFERENCES csv_info(import_id) ON DELETE CASCADE
                    );

                    CREATE TABLE source (
                        id UUID PRIMARY KEY,
                        dimension_id UUID,
                        import_id UUID UNIQUE,
                        revision_id UUID,
                        lookup_table_revision_id UUID,
                        csv_field TEXT,
                        action VARCHAR(255) NOT NULL,
                        FOREIGN KEY (dimension_id) REFERENCES dimension(id) ON DELETE CASCADE,
                        FOREIGN KEY (import_id) REFERENCES import(id) ON DELETE CASCADE,
                        FOREIGN KEY (revision_id) REFERENCES revision(id) ON DELETE CASCADE,
                        FOREIGN KEY (lookup_table_revision_id) REFERENCES revision(id) ON DELETE SET NULL
                    );
                `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
                    DROP TABLE source;
                    DROP TABLE import;
                    DROP TABLE csv_info;
                    DROP TABLE dimension;
                    DROP TABLE revision;
                    DROP TABLE dimension_info;
                    DROP TABLE dataset_info;
                    DROP TABLE dataset;
                    DROP TABLE users;
                `);
    }
}
