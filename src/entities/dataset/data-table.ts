import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    JoinColumn,
    OneToMany,
    OneToOne
} from 'typeorm';

import { FileType } from '../../enums/file-type';
import { FactTableAction } from '../../enums/fact-table-action';

import { Revision } from './revision';
import { DataTableDescription } from './data-table-description';
import { FileImportInterface } from './file-import.interface';

@Entity({ name: 'data_table', orderBy: { uploadedAt: 'ASC' } })
export class DataTable extends BaseEntity implements FileImportInterface {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_data_table_id' })
    id: string;

    @OneToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_data_table_revision_id' })
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

    @Column({ type: 'enum', enum: Object.values(FactTableAction), nullable: false })
    action: FactTableAction;

    @OneToMany(() => DataTableDescription, (factTableInfo) => factTableInfo.factTable, { cascade: true })
    dataTableDescriptions: DataTableDescription[];
}
