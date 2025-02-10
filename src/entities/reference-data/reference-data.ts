import { Entity, BaseEntity, PrimaryColumn, ManyToOne, JoinColumn, Column, Index } from 'typeorm';

import { CategoryKey } from './category-key';

@Entity('reference_data')
export class ReferenceData extends BaseEntity {
    @PrimaryColumn({ name: 'item_id', type: 'text' })
    itemId: string;

    @PrimaryColumn({ name: 'version_no', type: 'int' })
    versionNumber: number;

    @Index('IDX_reference_data_category_key')
    @PrimaryColumn({ name: 'category_key', type: 'text' })
    categoryKey: string;

    @Column({ name: 'sort_order', type: 'int', nullable: true })
    sortOrder: number | null;

    @Column({ name: 'validity_start', type: 'date' })
    validityStart: Date;

    @Column({ name: 'validity_end', type: 'date', nullable: true })
    validityEnd: Date;

    @ManyToOne(() => CategoryKey)
    @JoinColumn({ name: 'category_key', foreignKeyConstraintName: 'FK_reference_data_category_key' })
    categoryKeyEntity: CategoryKey;
}
