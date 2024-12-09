import {
    BaseEntity,
    Column,
    CreateDateColumn,
    Entity,
    JoinColumn,
    ManyToOne,
    PrimaryColumn,
    UpdateDateColumn
} from 'typeorm';

import { Team } from './team';

@Entity({ name: 'team_info' })
export class TeamInfo extends BaseEntity {
    @PrimaryColumn({
        name: 'team_id',
        type: 'uuid',
        primaryKeyConstraintName: 'PK_team_info_team_id_language'
    })
    id: string;

    @ManyToOne(() => Team, (team) => team.info, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
    @JoinColumn({ name: 'team_id', foreignKeyConstraintName: 'FK_team_info_team_id' })
    team: Team;

    @PrimaryColumn({
        name: 'language',
        type: 'varchar',
        length: 5,
        primaryKeyConstraintName: 'PK_team_info_team_id_language'
    })
    language: string;

    @Column({ name: 'name', type: 'text', nullable: true })
    name: string;

    @Column({ name: 'email', type: 'text', nullable: false })
    email: string;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz' })
    updatedAt: Date;
}
