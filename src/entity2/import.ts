import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    BaseEntity,
    OneToOne,
    ManyToOne,
    OneToMany,
    JoinColumn
} from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { RevisionEntity } from './revision';
// eslint-disable-next-line import/no-cycle
import { CsvInfo } from './csv_info';
// eslint-disable-next-line import/no-cycle
import { Source } from './source';

@Entity()
export class Import extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => RevisionEntity)
    @JoinColumn({ name: 'revision_id' })
    revision: RevisionEntity;

    @OneToOne(() => CsvInfo, (csvInfo) => csvInfo.import, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'csv_info' })
    csvInfo: CsvInfo;

    @Column({ type: 'varchar', length: 255 })
    mime_type: string;

    @Column({ type: 'varchar', length: 255 })
    filename: string;

    @Column({ type: 'varchar', length: 255 })
    hash: string;

    @Column({ type: 'timestamp', default: () => 'CURRENT_TIMESTAMP' })
    uploaded_at: Date;

    @Column({ type: 'enum', enum: ['Draft', 'FactTable', 'LookupTable'], nullable: false })
    type: string;

    @Column({ type: 'enum', enum: ['BlobStorage', 'Datalake'], nullable: false })
    location: string;

    @OneToMany(() => Source, (source) => source.import)
    sources: Source[];
}
