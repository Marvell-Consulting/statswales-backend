import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToOne, JoinColumn } from 'typeorm';

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

    @OneToOne(() => Source, (source) => source.import)
    source: Source;
}
