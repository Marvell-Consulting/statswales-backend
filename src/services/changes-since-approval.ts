import { logger } from '../utils/logger';
import { DimensionRepository } from '../repositories/dimension';
import { PublishedDatasetRepository } from '../repositories/published-dataset';
import { RevisionList } from '../repositories/revision';

const UNDEFINED_TABLE = '42P01';

// The cube builder adds _1, _2... to a column name when two dimensions share a name in the same language.
const isSameColumnName = (cubeName: string, currentName: string): boolean => {
  if (cubeName === currentName) return true;
  return cubeName.startsWith(currentName) && /^_\d+$/.test(cubeName.slice(currentName.length));
};

// Dimension names and lookup tables belong to the dataset rather than the revision, so a draft update (or a
// deleted one) can change them after a revision was approved. Rebuilding that revision would then put the
// unpublished changes into its cube. Returns a description of each change found; empty means safe to rebuild.
// Measure names are not checked because the cube builder does not use them.
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

    const cubeNames = await PublishedDatasetRepository.getPublishedDimensionNames(revision.id);
    const dimensions = await DimensionRepository.getByDatasetId(revision.dataset_id);

    for (const dimension of dimensions) {
      const cubeRows = cubeNames.filter((row) => row.fact_table_column === dimension.factTableColumn);

      // updates must match the existing fact table columns, so a dimension missing from the cube is unexpected
      if (cubeRows.length === 0) {
        changes.push(`${dimension.factTableColumn} is not in the cube`);
        continue;
      }

      for (const meta of dimension.metadata ?? []) {
        // the cube builder falls back to the fact table column when a dimension has no name
        const currentName = meta.name || dimension.factTableColumn;
        const cubeName = cubeRows.find(
          (row) => row.language.toLowerCase() === meta.language.toLowerCase()
        )?.dimension_name;

        if (cubeName !== undefined && !isSameColumnName(cubeName, currentName)) {
          changes.push(`${dimension.factTableColumn} (${meta.language}) renamed after approval`);
        }
      }
    }
  } catch (err) {
    if ((err as { code?: string })?.code === UNDEFINED_TABLE) {
      changes.push('cube has no filter table, so names cannot be checked');
    } else {
      logger.warn(err, `Could not check revision ${revision.id} for changes since approval`);
      changes.push('could not check for changes since approval');
    }
  }

  return changes;
}
