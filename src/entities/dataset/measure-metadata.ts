import {
    Entity,
    PrimaryColumn,
    Column,
    BaseEntity,
    ManyToOne,
    JoinColumn,
    CreateDateColumn,
    UpdateDateColumn
} from 'typeorm';

import { Measure } from './measure';

@Entity({ name: 'measure_metadata' })
export class MeasureMetadata extends BaseEntity {
    @PrimaryColumn({
        name: 'measure_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_measure_metadata_measure_id_language'
    })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_measure_metadata_measure_id_language'
    })
    language: string;

    @Column({ type: 'text' })
    name: string;

    @Column({ type: 'text', nullable: true })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => Measure, (measure) => measure.metadata, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'measure_id', foreignKeyConstraintName: 'FK_measure_metadata_measure_id' })
    measure: Measure;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    // This column should be the same across all languages
    // If one is updated and the others aren't then mark as needing translation
    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
