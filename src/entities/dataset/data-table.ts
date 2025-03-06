import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, BaseEntity, OneToMany, OneToOne } from 'typeorm';

import { FileType } from '../../enums/file-type';
import { DataTableAction } from '../../enums/data-table-action';

import { Revision } from './revision';
import { DataTableDescription } from './data-table-description';
import { FileImportInterface } from './file-import.interface';

@Entity({ name: 'data_table', orderBy: { uploadedAt: 'ASC' } })
export class DataTable extends BaseEntity implements FileImportInterface {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_data_table_id' })
    id: string;

    @OneToOne(() => Revision, (revision) => revision.dataTable, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    revision: Revision;

    @Column({ name: 'mime_type', type: 'varchar', length: 255 })
    mimeType: string;

    @Column({ name: 'filetype', type: 'enum', enum: Object.values(FileType), nullable: false })
    fileType: FileType;

    @Column({ type: 'varchar', length: 255 })
    filename: string;

    @Column({ name: 'original_filename', type: 'varchar', length: 255 })
    originalFilename: string;

    @Column({ type: 'varchar', length: 255 })
    hash: string;

    @CreateDateColumn({ name: 'uploaded_at', type: 'timestamptz' })
    uploadedAt: Date;

    @Column({ type: 'enum', enum: Object.values(DataTableAction), nullable: false })
    action: DataTableAction;

    @OneToMany(() => DataTableDescription, (factTableInfo) => factTableInfo.factTable, { cascade: true })
    dataTableDescriptions: DataTableDescription[];
}
