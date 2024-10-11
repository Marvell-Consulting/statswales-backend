import { Entity, BaseEntity, PrimaryColumn } from 'typeorm';

@Entity({ name: 'category' })
export class Category extends BaseEntity {
    @PrimaryColumn({ type: 'text' })
    category: string;
}
