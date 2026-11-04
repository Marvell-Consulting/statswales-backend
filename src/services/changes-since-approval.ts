import { logger } from '../utils/logger';
import { DimensionRepository } from '../repositories/dimension';
import { PublishedDatasetRepository } from '../repositories/published-dataset';
import { RevisionList } from '../repositories/revision';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';
import { DimensionType } from '../enums/dimension-type';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { makeCubeSafeString } from './cube-builder';

const UNDEFINED_TABLE = '42P01';

// the cube builder copies the lookup table into the cube for these types only
const LOOKUP_TYPES: DimensionType[] = [DimensionType.LookupTable, DimensionType.Date, DimensionType.DatePeriod];

// raw and symbol dimensions write the de-duplicated column name to the filter table, the others write the name as entered
const SUFFIXED_NAME_TYPES: DimensionType[] = [DimensionType.Raw, DimensionType.Symbol];

class MissingFilterTableError extends Error {}

const getCubeNames = async (revisionId: string): Promise<FactTableToDimensionName[]> => {
  try {
    return await PublishedDatasetRepository.getPublishedDimensionNames(revisionId);
  } catch (err) {
    // TypeORM's QueryFailedError copies the driver's error code onto itself
    if ((err as { code?: string })?.code === UNDEFINED_TABLE) throw new MissingFilterTableError();
    throw err;
  }
};

// The cube builder adds _1, _2... to a column name when two dimensions share a name in the same language.
const isSameColumnName = (cubeName: string, currentName: string, mayHaveSuffix: boolean): boolean => {
  if (cubeName === currentName) return true;
  return mayHaveSuffix && cubeName.startsWith(currentName) && /^_\d+$/.test(cubeName.slice(currentName.length));
};

// Dimension names, types and lookup tables belong to the dataset rather than the revision, so a draft update (or a
// deleted one) can change them after a revision was approved. Rebuilding that revision would then put the
// unpublished changes into its cube. Returns a description of each change found; empty means safe to rebuild.
//
// Not covered: changes between numeric, text and raw types, and changes to a dimension's number or date format,
// as the cube keeps no record of them. Measure names are not checked because the cube builder does not use them.
export async function getChangesSinceApproval(revision: RevisionList): Promise<string[]> {
  if (!revision.approved_at) return [];

  const approvedAt = new Date(revision.approved_at);
  if (Number.isNaN(approvedAt.getTime())) {
    return [`approval time "${String(revision.approved_at)}" is not a valid date`];
  }

  const changes: string[] = [];

  try {
    const replacedLookups = await DimensionRepository.getColumnsWithLookupsUploadedSince(
      revision.dataset_id,
      approvedAt
    );
    changes.push(...replacedLookups.map((column) => `lookup table for ${column} replaced after approval`));

    const cubeNames = await getCubeNames(revision.id);
    const cubeTables = await PublishedDatasetRepository.getCubeTableNames(revision.id);
    const dimensions = await DimensionRepository.getByDatasetId(revision.dataset_id);

    // the cube builder names each dimension in every supported locale, falling back to the fact table column
    const currentNames = dimensions.flatMap((dimension) =>
      SUPPORTED_LOCALES.map((locale) => ({
        column: dimension.factTableColumn,
        language: locale.toLowerCase(),
        name: dimension.metadata?.find((meta) => meta.language === locale)?.name || dimension.factTableColumn
      }))
    );

    for (const dimension of dimensions) {
      const cubeRows = cubeNames.filter((row) => row.fact_table_column === dimension.factTableColumn);

      // updates must match the existing fact table columns, so a dimension missing from the cube is unexpected
      if (cubeRows.length === 0) {
        changes.push(`${dimension.factTableColumn} is not in the cube`);
        continue;
      }

      const cubeHasLookup = cubeTables.includes(`${makeCubeSafeString(dimension.factTableColumn)}_lookup`);
      if (cubeHasLookup !== LOOKUP_TYPES.includes(dimension.type)) {
        changes.push(`${dimension.factTableColumn} type changed after approval`);
      }

      for (const current of currentNames.filter((entry) => entry.column === dimension.factTableColumn)) {
        const cubeName = cubeRows.find((row) => row.language === current.language)?.dimension_name;

        if (cubeName === undefined) {
          changes.push(`${current.column} (${current.language}) has no name in the cube`);
          continue;
        }

        const mayHaveSuffix =
          SUFFIXED_NAME_TYPES.includes(dimension.type) &&
          currentNames.filter((entry) => entry.language === current.language && entry.name === current.name).length > 1;

        if (!isSameColumnName(cubeName, current.name, mayHaveSuffix)) {
          changes.push(`${current.column} (${current.language}) renamed after approval`);
        }
      }
    }
  } catch (err) {
    if (err instanceof MissingFilterTableError) {
      changes.push('cube has no filter table, so names cannot be checked');
    } else {
      logger.warn(err, `Could not check revision ${revision.id} for changes since approval`);
      changes.push('could not check for changes since approval');
    }
  }

  return changes;
}
