import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    ManyToOne,
    OneToMany,
    JoinColumn
} from 'typeorm';

import { ImportType } from '../enums/import-type';
import { DataLocation } from '../enums/data-location';

// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
// eslint-disable-next-line import/no-cycle
import { CsvInfo } from './csv-info';
// eslint-disable-next-line import/no-cycle
import { Source } from './source';

@Entity({ name: 'file_import', orderBy: { uploadedAt: 'ASC' } })
export class FileImport extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_import_id' })
    id: string;

    @ManyToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_import_revision_id' })
    revision: Promise<Revision>;

    @OneToMany(() => CsvInfo, (csvInfo) => csvInfo.import, { cascade: true })
    @JoinColumn({ name: 'csv_info', foreignKeyConstraintName: 'FK_import_csv_info' })
    csvInfo: Promise<CsvInfo[]>;

    @Column({ name: 'mime_type', type: 'varchar', length: 255 })
    mimeType: string;

    @Column({ type: 'varchar', length: 255 })
    filename: string;

    @Column({ type: 'varchar', length: 255 })
    hash: string;

    @CreateDateColumn({ name: 'uploaded_at', type: 'timestamptz' })
    uploadedAt: Date;

    @Column({ type: 'enum', enum: Object.values(ImportType), nullable: false })
    type: string;

    @Column({ type: 'enum', enum: Object.values(DataLocation), nullable: false })
    location: string;

    @OneToMany(() => Source, (source) => source.import, { cascade: true })
    sources: Promise<Source[]>;
}
