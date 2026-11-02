import { logger } from '../utils/logger';
import { DimensionRepository } from '../repositories/dimension';
import { PublishedDatasetRepository } from '../repositories/published-dataset';
import { RevisionList } from '../repositories/revision';

// Dimension names and lookup tables belong to the dataset rather than the revision, so a draft update (or a
// deleted one) can change them after a revision was approved. Rebuilding that revision would then put the
// unpublished changes into its cube. Returns a description of each change found; empty means safe to rebuild.
export async function getChangesSinceApproval(revision: RevisionList): Promise<string[]> {
  if (!revision.approved_at) return [];

  try {
    const changes: string[] = [];
    const approvedAt = new Date(revision.approved_at);

    const replacedLookups = await DimensionRepository.getColumnsWithLookupsUploadedSince(
      revision.dataset_id,
      approvedAt
    );
    changes.push(...replacedLookups.map((column) => `lookup table for ${column} replaced after approval`));

    const cubeNames = await PublishedDatasetRepository.getPublishedDimensionNames(revision.id);
    const dimensions = await DimensionRepository.getByDatasetId(revision.dataset_id);

    for (const dimension of dimensions) {
      for (const meta of dimension.metadata ?? []) {
        // the cube builder falls back to the fact table column when a dimension has no name
        const currentName = meta.name || dimension.factTableColumn;
        const cubeName = cubeNames.find(
          (row) =>
            row.fact_table_column === dimension.factTableColumn &&
            row.language.toLowerCase() === meta.language.toLowerCase()
        )?.dimension_name;

        if (cubeName !== undefined && cubeName !== currentName) {
          changes.push(`${dimension.factTableColumn} (${meta.language}) renamed after approval`);
        }
      }
    }

    return changes;
  } catch (err) {
    logger.warn(err, `Could not check revision ${revision.id} for changes since approval`);
    return ['could not check for changes since approval'];
  }
}
