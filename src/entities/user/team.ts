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
import { TeamInfo } from './team-info';

@Entity({ name: 'team' })
export class Team extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_team_id' })
    id: string;

    @Column({ name: 'prefix', type: 'text', nullable: false })
    prefix: string;

    @OneToMany(() => TeamInfo, (info) => info.team, { cascade: true })
    info: TeamInfo[];

    @ManyToOne(() => Organisation)
    @JoinColumn({ name: 'organisation_id', foreignKeyConstraintName: 'FK_team_organisation_id' })
    organisation?: Organisation;

    @OneToMany(() => Dataset, (dataset) => dataset.team)
    datasets?: Dataset[];

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
