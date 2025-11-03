import { Router } from 'express';
import { getBuildLog, getBuiltLogEntry } from '../controllers/build-log';

export const buildLogRouter = Router();

// GET /build/
// Returns the most recent build log entries.
// query params:
// - size: number of entries to return (default 30)
// - page: page offset, allows paging the build log
buildLogRouter.get('/', getBuildLog);

// GET /build/id/:build_id/
// Returns a full specific entry from the build log
buildLogRouter.get('/:build_id', getBuiltLogEntry);
