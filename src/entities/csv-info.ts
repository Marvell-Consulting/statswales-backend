import { Entity, PrimaryColumn, Column, BaseEntity, ManyToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { FileImport } from './file-import';

@Entity({ name: 'csv_info' })
export class CsvInfo extends BaseEntity {
    @PrimaryColumn({ name: 'import_id', type: 'uuid', primaryKeyConstraintName: 'PK_csv_info_import_id' })
    id: string;

    @Column({ type: 'varchar', length: 1 })
    delimiter: string;

    @Column({ type: 'varchar', length: 1 })
    quote: string;

    @Column({ type: 'varchar', length: 2 })
    linebreak: string;

    @ManyToOne(() => FileImport, (fileImport) => fileImport.csvInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'import_id', foreignKeyConstraintName: 'FK_csv_info_import_id' })
    import: Promise<FileImport>;
}
