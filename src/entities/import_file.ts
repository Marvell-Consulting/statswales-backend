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

// eslint-disable-next-line import/no-cycle
import { Revision } from './revision';
// eslint-disable-next-line import/no-cycle
import { CsvInfo } from './csv_info';
// eslint-disable-next-line import/no-cycle
import { Source } from './source';

@Entity({ name: 'file_import', orderBy: { uploadedAt: 'ASC' } })
export class FileImport extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => Revision, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'revision_id' })
    revision: Promise<Revision>;

    @OneToMany(() => CsvInfo, (csvInfo) => csvInfo.import, {
        cascade: true
    })
    @JoinColumn({ name: 'csv_info' })
    csvInfo: Promise<CsvInfo[]>;

    @Column({ type: 'varchar', length: 255 })
    mime_type: string;

    @Column({ type: 'varchar', length: 255 })
    filename: string;

    @Column({ type: 'varchar', length: 255 })
    hash: string;

    @CreateDateColumn({ name: 'uploaded_at' })
    uploadedAt: Date;

    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: ['Draft', 'FactTable', 'LookupTable'],
        nullable: false
    })
    type: string;

    @Column({
        type: process.env.NODE_ENV === 'test' ? 'text' : 'enum',
        enum: ['BlobStorage', 'Datalake', 'Unknown'],
        nullable: false
    })
    location: string;

    @OneToMany(() => Source, (source) => source.import, {
        cascade: true
    })
    sources: Promise<Source[]>;
}
