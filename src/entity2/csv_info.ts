import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, OneToOne, JoinColumn } from 'typeorm';

// eslint-disable-next-line import/no-cycle
import { Import } from './import';

@Entity()
export class CsvInfo extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    import_id: string;

    @Column({ type: 'char', length: 1 })
    delimiter: string;

    @Column({ type: 'char', length: 1 })
    quote: string;

    @Column({ type: 'varchar', length: 2 })
    linebreak: string;

    @OneToOne(() => Import, (importEntity) => importEntity.csvInfo, { onDelete: 'CASCADE' })
    @JoinColumn({ name: 'import_id' })
    import: Import;
}
