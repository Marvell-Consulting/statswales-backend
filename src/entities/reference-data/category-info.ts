import { Entity, BaseEntity, PrimaryColumn, ManyToOne, JoinColumn, Column } from 'typeorm';

import { Category } from './category';

@Entity('category_info')
export class CategoryInfo extends BaseEntity {
    @PrimaryColumn({ type: 'text' })
    category: string;

    @PrimaryColumn({ type: 'text' })
    lang: string;

    @Column({ type: 'text' })
    description: string;

    @Column({ type: 'text', nullable: true })
    notes: string;

    @ManyToOne(() => Category)
    @JoinColumn({ name: 'category', foreignKeyConstraintName: 'FK_category_info_category' })
    categoryEntity: Category;
}
