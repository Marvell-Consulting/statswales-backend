import { BuildLog } from '../entities/dataset/build-log';
import { CubeBuildStatus } from '../enums/cube-build-status';
import { CubeBuildType } from '../enums/cube-build-type';

export class BuiltLogEntryDto {
  id: string;
  status: CubeBuildStatus;
  type: CubeBuildType;
  revision_id?: string;
  userId?: string;
  startedAt: Date;
  completedAt?: Date;
  performanceStart: number;
  performanceFinish?: number;
  duration_ms?: number;
  buildScript?: string;
  errors?: string;

  static fromBuildLogLite(buildLog: BuildLog): BuiltLogEntryDto {
    const dto = new BuiltLogEntryDto();
    dto.id = buildLog.id;
    dto.status = buildLog.status;
    dto.type = buildLog.type;
    dto.startedAt = buildLog.startedAt;
    dto.completedAt = buildLog.completedAt ? buildLog.completedAt : undefined;
    return dto;
  }

  static fromBuildLogFull(buildLog: BuildLog): BuiltLogEntryDto {
    const dto = BuiltLogEntryDto.fromBuildLogLite(buildLog);
    dto.buildScript = buildLog.buildScript ? buildLog.buildScript : undefined;
    if (buildLog.errors) {
      try {
        dto.errors = JSON.parse(buildLog.errors);
      } catch (_) {
        dto.errors = buildLog.errors;
      }
    }
    dto.performanceStart = buildLog.performanceStart;
    dto.performanceFinish = buildLog.performanceFinish ? buildLog.performanceFinish : undefined;
    dto.duration_ms = buildLog.duration ? buildLog.duration : undefined;
    dto.revision_id = buildLog.revisionId ? buildLog.revisionId : undefined;
    return dto;
  }
}
