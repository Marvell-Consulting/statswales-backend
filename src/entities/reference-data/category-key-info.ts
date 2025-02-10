import { Entity, BaseEntity, Column, PrimaryColumn, ManyToOne, JoinColumn } from 'typeorm';

import { CategoryKey } from './category-key';

@Entity('category_key_info')
export class CategoryKeyInfo extends BaseEntity {
    @PrimaryColumn({ name: 'category_key', type: 'text' })
    categoryKey: string;

    @PrimaryColumn({ type: 'text' })
    lang: string;

    @Column({ type: 'text' })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => CategoryKey)
    @JoinColumn({ name: 'category_key', foreignKeyConstraintName: 'FK_category_key_info_category_key' })
    categoryKeyEntity: CategoryKey;
}
