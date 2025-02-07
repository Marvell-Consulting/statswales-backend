import { Entity, BaseEntity, Column, PrimaryColumn, ManyToOne, JoinColumn } from 'typeorm';

import { Category } from './category';

@Entity('category_key')
export class CategoryKey extends BaseEntity {
    @PrimaryColumn({ name: 'category_key', type: 'text' })
    categoryKey: string;

    @Column({ type: 'text' })
    category: string;

    @ManyToOne(() => Category)
    @JoinColumn({ name: 'category', foreignKeyConstraintName: 'FK_category_key_category' })
    categoryEntity: Category;
}
