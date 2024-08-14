import { Entity, PrimaryGeneratedColumn, Column, BaseEntity, ManyToOne, OneToOne, JoinColumn } from 'typeorm';

import { Dimension } from './dimension';
// eslint-disable-next-line import/no-cycle
import { Import } from './import';
import { RevisionEntity } from './revision';

@Entity()
export class Source extends BaseEntity {
    @PrimaryGeneratedColumn('uuid')
    id: string;

    @ManyToOne(() => Dimension)
    @JoinColumn({ name: 'dimension_id' })
    dimension: Dimension;

    @OneToOne(() => Import, (importEntity) => importEntity.source, { nullable: false })
    @JoinColumn({ name: 'import_id' })
    import: Import;

    @ManyToOne(() => RevisionEntity)
    @JoinColumn({ name: 'revision_id' })
    revision: RevisionEntity;

    @ManyToOne(() => RevisionEntity)
    @JoinColumn({ name: 'lookup_table_revision_id' })
    lookupTableRevision: RevisionEntity;

    @Column({ type: 'text' })
    csv_field: string;

    // Replace with actual enum types
    @Column({ type: 'enum', enum: ['action1', 'action2'], nullable: false })
    action: string;
}
