import { MigrationInterface, QueryRunner } from 'typeorm';

export class Migration1723729297617 implements MigrationInterface {
    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`
                    CREATE TABLE users (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        email VARCHAR(255) NOT NULL UNIQUE,
                        oidc_subject VARCHAR(255) UNIQUE,
                        oidc_issuer VARCHAR(255),
                        access_token TEXT,
                        refresh_token TEXT,
                        id_token TEXT,
                        token_expiry TIMESTAMPTZ,
                        name VARCHAR(255),
                        given_name VARCHAR(255),
                        last_name VARCHAR(255),
                        profile_picture VARCHAR(255),
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        active BOOLEAN NOT NULL DEFAULT true
                    );

                    CREATE TABLE dataset (
                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                        creation_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        created_by UUID,
                        live TIMESTAMPTZ,
                        archive TIMESTAMPTZ,
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    );

                    CREATE TABLE dataset_info (
                        dataset_id UUID,
                        language VARCHAR(5),
                        title TEXT,
                        description TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ,
                        PRIMARY KEY (dataset_id, language),
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE
                    );

                    CREATE TABLE revision (
                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                        revision_index INT,
                        dataset_id UUID,
                        creation_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        previous_revision_id UUID,
                        online_cube_filename VARCHAR(255),
                        publish_date TIMESTAMPTZ,
                        approval_date TIMESTAMPTZ,
                        approved_by UUID,
                        created_by UUID,
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE,
                        FOREIGN KEY (previous_revision_id) REFERENCES revision(id) ON DELETE SET NULL,
                        FOREIGN KEY (approved_by) REFERENCES users(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    );

                    CREATE TYPE dimension_type AS ENUM ('RAW', 'TEXT', 'NUMERIC', 'SYMBOL', 'LOOKUP_TABLE', 'TIME_PERIOD', 'TIME_POINT');

                    CREATE TABLE dimension (
                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                        dataset_id UUID,
                        type dimension_type NOT NULL,
                        start_revision_id UUID NOT NULL,
                        finish_revision_id UUID,
                        validator TEXT,
                        FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE,
                        FOREIGN KEY (start_revision_id) REFERENCES revision(id) ON DELETE CASCADE,
                        FOREIGN KEY (finish_revision_id) REFERENCES revision(id) ON DELETE SET NULL
                    );

                    CREATE TABLE dimension_info (
                        dimension_id UUID,
                        language VARCHAR(5),
                        name TEXT,
                        description TEXT,
                        notes TEXT,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ,
                        PRIMARY KEY (dimension_id, language),
                        FOREIGN KEY (dimension_id) REFERENCES dimension(id) ON DELETE CASCADE
                    );

                    CREATE TYPE import_type AS ENUM ('Draft', 'FactTable', 'LookupTable');
                    CREATE TYPE location_type AS ENUM ('BlobStorage', 'Datalake');

                    CREATE TABLE import (
                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                        revision_id UUID,
                        mime_type VARCHAR(255),
                        filename VARCHAR(255),
                        hash VARCHAR(255),
                        uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        type import_type NOT NULL,
                        location location_type NOT NULL,
                        FOREIGN KEY (revision_id) REFERENCES revision(id) ON DELETE CASCADE
                    );

                    CREATE TABLE csv_info (
                        import_id UUID PRIMARY KEY,
                        delimiter CHAR(1),
                        quote CHAR(1),
                        linebreak VARCHAR(2),
                        FOREIGN KEY (import_id) REFERENCES import(id) ON DELETE CASCADE
                    );

                    CREATE TYPE source_type as ENUM ('Unknown', 'DataValues', 'FootNotes', 'Dimension', 'IGNORE');
                    CREATE TYPE source_action_type AS ENUM ('unknwon', 'create', 'append', 'truncate-then-load', 'ignore');

                    CREATE TABLE source (
                        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                        dimension_id UUID,
                        import_id UUID,
                        revision_id UUID,
                        column_index INT,
                        csv_field TEXT,
                        action source_action_type NOT NULL,
                        type source_type DEFAULT 'Unknown',
                        FOREIGN KEY (dimension_id) REFERENCES dimension(id) ON DELETE CASCADE,
                        FOREIGN KEY (import_id) REFERENCES import(id) ON DELETE CASCADE,
                        FOREIGN KEY (revision_id) REFERENCES revision(id) ON DELETE CASCADE
                    );

                    INSERT INTO users (
                        id,
                        email,
                        oidc_subject,
                        oidc_issuer,
                        access_token,
                        refresh_token,
                        id_token,
                        token_expiry,
                        name,
                        given_name,
                        last_name,
                        profile_picture,
                        created_at,
                        updated_at,
                        active
                    )
                    VALUES (
                        '12345678-1234-1234-1234-123456789012',
                        'test@test.com',
                        '',
                        'localAuth',
                        '',
                        '',
                        '',
                        NULL,
                        'Test User',
                        'Test',
                        'User',  -- Corrected by closing the quote
                        '',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        true
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
