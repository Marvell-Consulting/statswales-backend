import { Entity, BaseEntity, PrimaryColumn } from 'typeorm';

@Entity({ name: 'categories' })
export class Category extends BaseEntity {
    @PrimaryColumn({ type: 'text' })
    category: string;
}
