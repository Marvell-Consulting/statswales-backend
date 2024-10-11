import { Entity, BaseEntity, PrimaryColumn, ManyToOne, JoinColumn, Column } from 'typeorm';

import { ReferenceData } from './reference-data';

@Entity('reference_data_info')
export class ReferenceDataInfo extends BaseEntity {
    @PrimaryColumn({ name: 'item_id', type: 'text' })
    itemId: string;

    @PrimaryColumn({ name: 'version_no', type: 'int' })
    versionNumber: number;

    @PrimaryColumn({ name: 'category_key', type: 'text' })
    categoryKey: string;

    @PrimaryColumn({ type: 'text' })
    lang: string;

    @Column({ type: 'text' })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => ReferenceData)
    @JoinColumn([
        { name: 'item_id', referencedColumnName: 'itemId' },
        { name: 'version_no', referencedColumnName: 'versionNumber' },
        { name: 'category_key', referencedColumnName: 'categoryKey' }
    ])
    referenceData: ReferenceData;
}
