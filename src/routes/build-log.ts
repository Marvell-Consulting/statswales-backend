import { Router } from 'express';
import {
  getBaseCubeBuildLog,
  getBuildLog,
  getBuiltLogEntry,
  getBulkAllBuildLog,
  getBulkDraftBuildLog,
  getCompletedBuildLog,
  getFailedBuildLog,
  getFullCubeBuildLog,
  getValidationCubeBuildLog
} from '../controllers/build-log';

export const buildLogRouter = Router();

// GET /build/
// Returns the most recent build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/', getBuildLog);

// GET /build/status/failed
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/status/failed', getFailedBuildLog);

// GET /build/status/completed
// Returns the most recent completed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/status/completed', getCompletedBuildLog);

// GET /build/type/base
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/type/base', getBaseCubeBuildLog);

// GET /build/type/validation
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/type/validation', getValidationCubeBuildLog);

// GET /build/type/full
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/type/full', getFullCubeBuildLog);

// GET /build/type/drafts
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/type/drafts', getBulkDraftBuildLog);

// GET /build/type/all
// Returns the most recent failed build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/type/all', getBulkAllBuildLog);

// GET /build/id/:build_id/
// Returns a full specific entry from the build log
buildLogRouter.get('/entry/:build_id', getBuiltLogEntry);
