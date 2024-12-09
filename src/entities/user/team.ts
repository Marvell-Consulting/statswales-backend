import {
    BaseEntity,
    Column,
    CreateDateColumn,
    Entity,
    JoinColumn,
    ManyToOne,
    OneToMany,
    PrimaryGeneratedColumn,
    UpdateDateColumn
} from 'typeorm';

import { Dataset } from '../dataset/dataset';

import { Organisation } from './organisation';

// TODO: some fields are temporarily nullable until we have the full information

@Entity({ name: 'team' })
export class Team extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_team_id' })
    id: string;

    @Column({ name: 'prefix', type: 'text', nullable: true })
    prefix?: string;

    @Column({ name: 'name_en', type: 'text', nullable: true })
    nameEN?: string;

    @Column({ name: 'name_cy', type: 'text', nullable: true })
    nameCY?: string;

    @Column({ name: 'email_en', type: 'text', nullable: false })
    emailEN: string;

    @Column({ name: 'email_cy', type: 'text', nullable: true })
    emailCY?: string;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;

    @ManyToOne(() => Organisation)
    @JoinColumn({ name: 'organisation_id', foreignKeyConstraintName: 'FK_team_organisation_id' })
    organisation?: Organisation;

    @OneToMany(() => Dataset, (dataset) => dataset.team)
    datasets: Dataset[];
}
