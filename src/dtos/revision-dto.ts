import { Revision } from '../entities/revision';
import { FileImport } from '../entities/file-import';

import { ImportDTO } from './fileimport-dto';

export class RevisionDTO {
    id: string;
    revision_index: number;
    creation_date: string;
    previous_revision_id?: string;
    online_cube_filename?: string;
    publish_date?: string;
    approval_date?: string;
    approved_by?: string;
    created_by: string;
    imports: ImportDTO[];
    dataset_id?: string;

    static async fromRevision(revision: Revision): Promise<RevisionDTO> {
        const revDto = new RevisionDTO();
        revDto.id = revision.id;
        revDto.revision_index = revision.revisionIndex;
        revDto.dataset_id = (await revision.dataset).id;
        revDto.creation_date = revision.creationDate.toISOString();
        revDto.previous_revision_id = (await revision.previousRevision)?.id;
        revDto.online_cube_filename = revision.onlineCubeFilename;
        revDto.publish_date = revision.publishDate?.toISOString();
        revDto.approval_date = revision.approvalDate?.toISOString();
        revDto.approved_by = (await revision.approvedBy)?.name || undefined;
        revDto.created_by = (await revision.createdBy).name;
        return revDto;
    }

    static async fromRevisionWithImports(revision: Revision): Promise<RevisionDTO> {
        const revDto = await RevisionDTO.fromRevision(revision);
        revDto.imports = await Promise.all(
            (await revision.imports).map(async (imp: FileImport) => {
                const impDto = await ImportDTO.fromImport(imp);
                return impDto;
            })
        );
        return revDto;
    }

    static async fromRevisionWithImportsAndSources(revision: Revision): Promise<RevisionDTO> {
        const revDto = await RevisionDTO.fromRevision(revision);
        revDto.imports = await Promise.all(
            (await revision.imports).map(async (imp: FileImport) => {
                const impDto = await ImportDTO.fromImportWithSources(imp);
                return impDto;
            })
        );
        return revDto;
    }
}
