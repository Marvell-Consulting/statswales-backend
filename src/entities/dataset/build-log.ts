import { BaseEntity, Column, CreateDateColumn, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';
import { Revision } from './revision';
import { CubeBuildStatus } from '../../enums/cube-build-status';
import { CubeBuildType } from '../../enums/cube-build-type';

@Entity({ name: 'build_log', orderBy: { startedAt: 'DESC' } })
export class BuildLog extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_build_log_id' })
  id: string;

  @Column({ name: 'status', type: 'enum', enum: Object.values(CubeBuildStatus), nullable: false })
  status: CubeBuildStatus;

  @Column({ name: 'type', type: 'enum', enum: Object.values(CubeBuildType), nullable: false })
  type: CubeBuildType;

  @CreateDateColumn({ name: 'started_at', type: 'timestamptz', nullable: false })
  startedAt: Date;

  @Column({ name: 'completed_at', type: 'timestamptz', nullable: true })
  completedAt: Date | null;

  @Column({ type: 'text', nullable: true, name: 'build_script' })
  buildScript: string | null;

  @Column({ type: 'text', nullable: true, name: 'errors' })
  errors: string;

  @ManyToOne(() => Revision, (revision) => revision.builds, { onDelete: 'CASCADE', orphanedRowAction: 'delete' })
  @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_revision_build_log_id' })
  revision: Revision;

  public static async startBuild(revision: Revision, type: CubeBuildType, buildId?: string): Promise<BuildLog> {
    const build = new BuildLog();
    if (buildId) build.id = buildId;
    build.revision = revision;
    build.type = type;
    build.status = CubeBuildStatus.Queued;
    return await build.save();
  }
}
