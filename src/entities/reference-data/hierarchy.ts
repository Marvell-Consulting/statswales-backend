import { Entity, BaseEntity, PrimaryColumn, ManyToOne, JoinColumn, Index } from 'typeorm';

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

    @Index('IDX_hierarchy_item_id_version_no_category_key', ['item_id', 'version_no', 'category_key'])
    @ManyToOne(() => ReferenceData)
    @JoinColumn([
        { name: 'item_id', referencedColumnName: 'itemId' },
        { name: 'version_no', referencedColumnName: 'versionNumber' },
        { name: 'category_key', referencedColumnName: 'categoryKey' }
    ])
    referenceData: ReferenceData;

    @Index('IDX_hierarchy_parent_id_parent_version_parent_category', ['parent_id', 'parent_version', 'parent_category'])
    @ManyToOne(() => ReferenceData)
    @JoinColumn([
        { name: 'parent_id', referencedColumnName: 'itemId' },
        { name: 'parent_version', referencedColumnName: 'versionNumber' },
        { name: 'parent_category', referencedColumnName: 'categoryKey' }
    ])
    parentReferenceData: ReferenceData;
}
