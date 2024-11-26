import {
    Entity,
    PrimaryColumn,
    Column,
    BaseEntity,
    ManyToOne,
    JoinColumn
} from 'typeorm';

import { Measure } from './measure';
import { DisplayType } from '../../enums/display-type';

@Entity({ name: 'measure_info' })
export class MeasureInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'measure_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_measure_info_measure_id_language'
    })
    id: string;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_measure_info_measure_id_language'
    })
    language: string;

    @Column({ type: 'varchar' })
    description: string;

    @Column({ type: 'varchar', nullable: false })
    reference: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => Measure, (measure) => measure.measureInfo, {
        onDelete: 'CASCADE',
        orphanedRowAction: 'delete'
    })
    @JoinColumn({ name: 'measure_id', foreignKeyConstraintName: 'FK_measure_info_measure_id' })
    measure: Measure;

    @Column({ name: 'display_type', type: 'enum', enum: Object.values(DisplayType), nullable: false })
    displayType: DisplayType;
}
