import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { FileImport } from './import_file';

@Entity()
export class CsvInfo extends BaseEntity {
    @PrimaryColumn({ name: 'import_id', type: process.env.NODE_ENV === 'test' ? 'text' : 'uuid' })
    id: string;

    @Column({ type: 'varchar', length: 1 })
    delimiter: string;

    @Column({ type: 'varchar', length: 1 })
    quote: string;

    @Column({ type: 'varchar', length: 2 })
    linebreak: string;

    @ManyToOne(() => FileImport, (importEntity) => importEntity.csvInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id' })
    import: Promise<FileImport>;
}
