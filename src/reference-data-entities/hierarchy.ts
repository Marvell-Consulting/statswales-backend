import { Entity, BaseEntity, PrimaryColumn, ManyToOne, JoinColumn, Column } from 'typeorm';

import { ReferenceData } from './reference-data';

@Entity('hierarchy')
export class Hierarchy extends BaseEntity {
    @PrimaryColumn({ name: 'item_id', type: 'text' })
    itemId: string;

    @PrimaryColumn({ name: 'version_no', type: 'int' })
    versionNumber: number;

    @PrimaryColumn({ name: 'category_key', type: 'text' })
    categoryKey: string;

    @PrimaryColumn({ name: 'parent_id', type: 'text' })
    parentId: string;

    @PrimaryColumn({ name: 'parent_version', type: 'int' })
    parentVersion: number;

    @PrimaryColumn({ name: 'parent_category', type: 'text' })
    parentCategory: string;

    @ManyToOne(() => ReferenceData)
    @JoinColumn([
        { name: 'item_id', referencedColumnName: 'itemId' },
        { name: 'version_no', referencedColumnName: 'versionNumber' },
        { name: 'category_key', referencedColumnName: 'categoryKey' }
    ])
    referenceData: ReferenceData;

    @ManyToOne(() => ReferenceData)
    @JoinColumn([
        { name: 'parent_id', referencedColumnName: 'itemId' },
        { name: 'parent_version', referencedColumnName: 'versionNumber' },
        { name: 'parent_category', referencedColumnName: 'categoryKey' }
    ])
    parentReferenceData: ReferenceData;
}
