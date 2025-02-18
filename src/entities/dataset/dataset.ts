import {
    Entity,
    PrimaryGeneratedColumn,
    Column,
    CreateDateColumn,
    BaseEntity,
    ManyToOne,
    OneToMany,
    JoinColumn,
    OneToOne,
    Index
} from 'typeorm';

import { User } from '../user/user';
import { Team } from '../user/team';

import { Revision } from './revision';
import { DatasetMetadata } from './dataset-metadata';
import { Dimension } from './dimension';
import { DatasetProvider } from './dataset-provider';
import { DatasetTopic } from './dataset-topic';
import { Measure } from './measure';
import { FactTableColumn } from './fact-table-column';

@Entity({ name: 'dataset' })
export class Dataset extends BaseEntity {
    @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_dataset_id' })
    id: string;

    @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
    createdAt: Date;

    @Index('IDX_dataset_created_by')
    @ManyToOne(() => User)
    @JoinColumn({ name: 'created_by', foreignKeyConstraintName: 'FK_dataset_created_by' })
    createdBy: User;

    @Column({ type: 'timestamptz', nullable: true })
    live: Date | null;

    @Column({ type: 'timestamptz', nullable: true })
    archive: Date;

    @Column({ name: 'start_date', type: 'date', nullable: true })
    startDate: Date | null;

    @Column({ name: 'end_date', type: 'date', nullable: true })
    endDate: Date | null;

    @OneToMany(() => DatasetMetadata, (metadata) => metadata.dataset, { cascade: true })
    metadata: DatasetMetadata[];

    @OneToMany(() => Dimension, (dimension) => dimension.dataset, { cascade: true })
    dimensions: Dimension[];

    @OneToMany(() => Revision, (revision) => revision.dataset, { cascade: true })
    revisions: Revision[];

    @OneToOne(() => Measure, (measure) => measure.dataset, { cascade: true })
    measure: Measure;

    @OneToMany(() => FactTableColumn, (factTableColumn) => factTableColumn.dataset, { cascade: true })
    factTable: FactTableColumn[] | null;

    @OneToMany(() => DatasetProvider, (datasetProvider) => datasetProvider.dataset, { cascade: true })
    datasetProviders: DatasetProvider[];

    @OneToMany(() => DatasetTopic, (datasetTopic) => datasetTopic.dataset, { cascade: true })
    datasetTopics: DatasetTopic[];

    @Column({ name: 'team_id', type: 'uuid', nullable: true })
    teamId?: string;

    @Index('IDX_dataset_team_id')
    @ManyToOne(() => Team, (team) => team.datasets, { nullable: true })
    @JoinColumn({ name: 'team_id', foreignKeyConstraintName: 'FK_dataset_team_id' })
    team: Team;
}
