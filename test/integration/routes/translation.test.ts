// SW-1278 — a publisher reports that the "Export text fields for translation"
// tasklist item flips back to Incomplete after they import their completed
// translation CSV. The unit-level tests in test/unit/dtos/tasklist-state-dto.test.ts
// cover the status-calculation logic in isolation with `collectTranslations` mocked.
// This file exercises the real round trip — datasetService.updateTranslations()
// persists values to the DB, getTasklistState() reads them back via the real
// collectTranslations() utility — which is where the bug would most plausibly hide.

import { dbManager } from '../../../src/db/database-manager';
import { initPassport } from '../../../src/middleware/passport-auth';
import { Dataset } from '../../../src/entities/dataset/dataset';
import { Revision } from '../../../src/entities/dataset/revision';
import { DimensionMetadata } from '../../../src/entities/dataset/dimension-metadata';
import { RevisionMetadata } from '../../../src/entities/dataset/revision-metadata';
import { EventLog } from '../../../src/entities/event-log';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { GroupRole } from '../../../src/enums/group-role';
import { Locale } from '../../../src/enums/locale';
import { TaskListStatus } from '../../../src/enums/task-list-status';
import { DatasetService } from '../../../src/services/dataset';
import { DatasetRepository, withMetadataForTranslation } from '../../../src/repositories/dataset';
import { TranslationDTO } from '../../../src/dtos/translations-dto';
import { collectTranslations } from '../../../src/utils/collect-translations';
import { getFileService } from '../../../src/utils/get-file-service';
import BlobStorage from '../../../src/services/blob-storage';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { createFullDataset } from '../../helpers/test-helper';
import { uuidV4 } from '../../../src/utils/uuid';

jest.mock('../../../src/services/blob-storage');
BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);
BlobStorage.prototype.saveBuffer = jest.fn();

const user: User = getTestUser('translation-test-user');
let userGroup = getTestUserGroup('Translation Test Group');
let datasetService: DatasetService;

interface SeededDataset {
  dataset: Dataset;
  revision: Revision;
}

// Creates a dataset with both English and Welsh metadata seeded for the dimensions
// and revision, and (optionally) a related link. updateTranslations() asserts that
// each dimension already has a `cy` metadata row, so we have to put one there.
async function seedDataset(opts: {
  title?: string;
  summary?: string;
  collection?: string;
  quality?: string;
  withRelatedLink?: boolean;
  dimensionColumnNames?: string[];
}): Promise<SeededDataset> {
  const datasetId = uuidV4();
  const revisionId = uuidV4();
  const dataTableId = uuidV4();

  await createFullDataset(datasetId, revisionId, dataTableId, user);

  const revision = await Revision.findOne({
    where: { id: revisionId },
    relations: { metadata: true }
  });
  if (!revision) throw new Error('seed: revision not found');

  // Fill every translatable metadata field with a realistic English value, so
  // exports won't contain null cells (which would get rejected by validation in
  // production anyway). The Welsh side starts blank — the round-trip simulates a
  // publisher who exports, fills in Welsh, and re-imports.
  const metaEn = revision.metadata.find((m) => m.language === Locale.EnglishGb)!;
  const metaCy = revision.metadata.find((m) => m.language === Locale.WelshGb)!;
  metaEn.title = opts.title ?? 'Healthy Child Wales Programme';
  metaCy.title = '';
  metaEn.summary = opts.summary ?? 'Annual percentage of incomplete contacts.';
  metaCy.summary = '';
  metaEn.collection = opts.collection ?? 'Data collection notes.';
  metaCy.collection = '';
  metaEn.quality = opts.quality ?? 'Quality statement.';
  metaCy.quality = '';
  await RevisionMetadata.getRepository().save([metaEn, metaCy]);

  if (opts.withRelatedLink) {
    revision.relatedLinks = [
      {
        id: uuidV4(),
        url: 'https://example.gov.wales/related',
        labelEN: "Publisher's notes",
        labelCY: '',
        created_at: new Date().toISOString()
      }
    ];
    await revision.save();
  }

  // createFullDataset only attaches English metadata to dimensions. updateTranslations
  // requires a Welsh row to exist for each dimension — create blank Welsh rows here.
  const dataset = await DatasetRepository.getById(datasetId, withMetadataForTranslation);
  for (const dimension of dataset.dimensions) {
    const hasCy = dimension.metadata.some((m) => m.language.includes('cy'));
    if (!hasCy) {
      const cyMeta = DimensionMetadata.create({
        id: dimension.id,
        language: 'cy-GB',
        name: ''
      } as DimensionMetadata);
      await DimensionMetadata.getRepository().save(cyMeta);
    }
  }

  // Reload to get the full state for the caller.
  const reloaded = await DatasetRepository.getById(datasetId, withMetadataForTranslation);
  return { dataset: reloaded, revision: reloaded.draftRevision! };
}

// Simulates the controller flow without going through HTTP file streaming / AV scan:
//   1. Save an `export` EventLog (as translationExport() does after streaming CSV).
//   2. Call datasetService.updateTranslations() with the filled-in translations
//      (as applyImport() does once the user has confirmed the validated upload).
//   3. Save an `import` EventLog (as applyImport() does after updateTranslations()).
async function simulateExportThenImport(
  revisionId: string,
  exportedTranslations: TranslationDTO[],
  importedTranslations: TranslationDTO[],
  exportTime: Date
): Promise<void> {
  await EventLog.getRepository().save({
    action: 'export',
    entity: 'translations',
    entityId: revisionId,
    data: exportedTranslations,
    userId: user.id,
    client: 'translation-integration-test',
    createdAt: exportTime
  });

  // Drive applyImport's key call.
  const dataset = await DatasetRepository.getById(
    (await Revision.findOneByOrFail({ id: revisionId })).datasetId,
    withMetadataForTranslation
  );
  await datasetService.updateTranslations(dataset.id, importedTranslations);

  await EventLog.getRepository().save({
    action: 'import',
    entity: 'translations',
    entityId: revisionId,
    data: importedTranslations,
    userId: user.id,
    client: 'translation-integration-test'
  });
}

function fillWelsh(translations: TranslationDTO[]): TranslationDTO[] {
  return translations.map((t) => ({
    ...t,
    cymraeg: t.cymraeg && t.cymraeg.length > 0 ? t.cymraeg : `${t.english ?? ''} (cy)`
  }));
}

describe('Translation round trip (SW-1278)', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();
    const fileService = getFileService();
    datasetService = new DatasetService(Locale.EnglishGb, fileService);
  });

  it('marks both export and import Completed after a clean round trip', async () => {
    const { dataset, revision } = await seedDataset({ title: 'My dataset' });
    const exported = collectTranslations(dataset);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Completed);
    expect(state.translation.export).toBe(TaskListStatus.Completed);
  });

  it('marks both Completed after a round trip with whitespace-padded metadata values', async () => {
    // If `stringify` writes the padded value verbatim and the CSV parser's
    // `trim: true` strips it, the imported event payload will not match the
    // stored values. The unit suite proves this would surface as Incomplete —
    // here we check whether the real round trip suffers from it.
    const { dataset, revision } = await seedDataset({ title: ' Healthy Child Wales Programme ' });
    const exported = collectTranslations(dataset);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Completed);
    expect(state.translation.export).toBe(TaskListStatus.Completed);
  });

  it('marks both Completed when metadata contains an apostrophe', async () => {
    const { dataset, revision } = await seedDataset({ title: "Children's wellbeing" });
    const exported = collectTranslations(dataset);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Completed);
    expect(state.translation.export).toBe(TaskListStatus.Completed);
  });

  it('marks both Completed when revision has a related link', async () => {
    const { dataset, revision } = await seedDataset({ title: 'With related links', withRelatedLink: true });
    const exported = collectTranslations(dataset);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Completed);
    expect(state.translation.export).toBe(TaskListStatus.Completed);
  });

  it('flips export Incomplete after the user edits metadata following a successful import', async () => {
    // This is the legitimate stale case. Pinned down to make sure a future fix
    // to SW-1278 does not over-correct and break it.
    const { dataset, revision } = await seedDataset({ title: 'Before edit' });
    const exported = collectTranslations(dataset);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    // User edits English title afterwards. RevisionMetadata's primary key column is
    // `revision_id` mapped to the property `id`.
    const metaEn = await RevisionMetadata.findOneByOrFail({
      id: revision.id,
      language: Locale.EnglishGb
    });
    metaEn.title = 'After edit';
    await metaEn.save();

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Incomplete);
    expect(state.translation.export).toBe(TaskListStatus.Incomplete);
  });

  it('marks both Completed for an update revision where the `reason` metadata key is in scope', async () => {
    // The reported dataset is an update (Healthy Child Wales Programme has a published
    // first revision and was being updated). On update revisions, `reason` joins the
    // translatable keys, and updateTranslations() persists it via a dynamic assignment
    // that the TS cast at services/dataset.ts:236 doesn't list — useful to confirm
    // the runtime behaviour is correct.
    const { dataset, revision } = await seedDataset({ title: 'Update revision title' });

    // Promote the seeded revision to look like an update: bump revisionIndex past 1.
    // collectTranslations() only checks revisionIndex to decide whether `reason` is in
    // scope; the previousRevisionId branch (the Unchanged short-circuit) only fires
    // when previousRevision is also loaded, which it isn't here.
    revision.revisionIndex = 2;
    await revision.save();

    // Reload through the same accessor the service uses.
    const reloadedDataset = await DatasetRepository.getById(dataset.id, withMetadataForTranslation);
    const metaEn = reloadedDataset.draftRevision!.metadata.find((m) => m.language === Locale.EnglishGb)!;
    metaEn.reason = 'Annual refresh';
    await metaEn.save();
    const metaCy = reloadedDataset.draftRevision!.metadata.find((m) => m.language === Locale.WelshGb)!;
    metaCy.reason = '';
    await metaCy.save();

    const refreshed = await DatasetRepository.getById(dataset.id, withMetadataForTranslation);
    const exported = collectTranslations(refreshed);
    const imported = fillWelsh(exported);

    await simulateExportThenImport(revision.id, exported, imported, new Date(Date.now() - 60_000));

    const state = await datasetService.getTasklistState(dataset.id, Locale.EnglishGb);

    expect(state.translation.import).toBe(TaskListStatus.Completed);
    expect(state.translation.export).toBe(TaskListStatus.Completed);
  });
});
