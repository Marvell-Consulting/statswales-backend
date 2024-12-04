import { BaseEntity, Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';

@Entity({ name: 'organisation' })
export class Organisation extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_organisation_id' })
    id: string;

    @Column({ name: 'name_en', type: 'text', nullable: false })
    nameEN: string;

    @Column({ name: 'name_cy', type: 'text', nullable: false })
    nameCY: string;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
