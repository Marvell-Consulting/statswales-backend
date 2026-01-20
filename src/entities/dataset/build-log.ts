import { BaseEntity, Column, CreateDateColumn, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';
import { Revision } from './revision';
import { CubeBuildStatus } from '../../enums/cube-build-status';
import { CubeBuildType } from '../../enums/cube-build-type';

export const CompleteStatus = [CubeBuildStatus.Completed, CubeBuildStatus.Failed];

@Entity({ name: 'build_log', orderBy: { startedAt: 'DESC' } })
export class BuildLog extends BaseEntity {
  @PrimaryGeneratedColumn('uuid', { primaryKeyConstraintName: 'PK_build_log_id' })
  id: string;

  @Column({ name: 'status', type: 'enum', enum: Object.values(CubeBuildStatus), nullable: false })
  status: CubeBuildStatus;

  @Column({ name: 'type', type: 'enum', enum: Object.values(CubeBuildType), nullable: false })
  type: CubeBuildType;

  @Column({ name: 'user_id', type: 'varchar', nullable: true })
  userId: string | null;

  @CreateDateColumn({ name: 'started_at', type: 'timestamptz', nullable: false })
  startedAt: Date;

  @Column({ name: 'completed_at', type: 'timestamptz', nullable: true })
  completedAt: Date | null;

  @Column({ name: 'performance_start', type: 'double precision' })
  performanceStart: number;

  @Column({ name: 'performance_finish', type: 'double precision', nullable: true })
  performanceFinish: number | null;

  @Column({ name: 'duration', type: 'double precision', nullable: true })
  duration: number | null;

  @Column({ type: 'text', nullable: true, name: 'build_script' })
  buildScript: string | null;

  @Column({ name: 'errors', type: 'text', nullable: true })
  errors: string | null;

  @Column({ name: 'revision_id', type: 'uuid', nullable: true })
  revisionId: string | null;

  @ManyToOne(() => Revision, (revision) => revision.builds, {
    onDelete: 'CASCADE',
    orphanedRowAction: 'delete',
    nullable: true
  })
  @JoinColumn({ name: 'revision_id', foreignKeyConstraintName: 'FK_revision_build_log_id' })
  revision: Revision | null;

  public static async startBuild(
    revision: Revision | null,
    type: CubeBuildType,
    userId?: string,
    buildId?: string
  ): Promise<BuildLog> {
    const build = BuildLog.create({
      id: buildId,
      revision,
      type,
      userId,
      status: CubeBuildStatus.Queued,
      startedAt: new Date(),
      performanceStart: performance.now()
    });
    return await build.save();
  }

  public completeBuild(status: CubeBuildStatus, buildScript?: string, errors?: string): void {
    this.status = status;
    if (buildScript) this.buildScript = buildScript;
    if (errors) this.errors = errors;
    this.duration = performance.now() - this.performanceStart;
    this.completedAt = new Date();
  }
}
