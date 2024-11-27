import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    ManyToOne,
    JoinColumn,
    OneToMany
} from 'typeorm';

import { FileType } from '../../enums/file-type';
import { Revision } from './revision';
import { FactTableInfo } from './fact-table-info';

@Entity({ name: 'fact_table', orderBy: { uploadedAt: 'ASC' } })
export class FactTable extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_fact_table_id' })
    id: string;

    @ManyToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_fact_table_revision_id' })
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

    @Column({ name: 'delimiter', type: 'char', nullable: true })
    delimiter: string;

    @Column({ name: 'quote', type: 'char', nullable: true })
    quote: string;

    @Column({ name: 'linebreak', type: 'varchar', nullable: true })
    linebreak: string;

    @OneToMany(() => FactTableInfo, (factTableInfo) => factTableInfo.factTable, { cascade: true })
    factTableInfo: FactTableInfo[];
}
