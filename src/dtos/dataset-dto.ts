/* eslint-disable import/no-cycle */
import { Dataset } from '../entity/dataset';

export interface DatafileDTO {
    id: string;
    sha256hash: string;
    created_by: string;
    creation_date: string;
}

export interface DatasetDescriptionDTO {
    description: string;
    language: string;
}

export interface DatasetTitleDTO {
    title: string;
    language: string;
}

export interface DatasetDTO {
    id: string;
    code: string;
    internal_name: string;
    title: DatasetTitleDTO[];
    description: DatasetDescriptionDTO[];
    creation_date: string;
    created_by: string;
    modification_date: string;
    modified_by: string;
    live: boolean;
    datafiles: DatafileDTO[];
    csv_link: string;
    xslx_link: string;
    view_link: string;
}

export async function datasetToDatasetDTO(dataset: Dataset): Promise<DatasetDTO> {
    const datasetTitleDtos: DatasetTitleDTO[] = [];
    const datasetDescriptionDtos: DatasetDescriptionDTO[] = [];
    const datafilesDtos: DatafileDTO[] = [];

    const titles = await dataset.title;
    for (const title of titles) {
        datasetTitleDtos.push({
            title: title.title,
            language: title.languageCode
        });
    }

    const descriptions = await dataset.description;
    for (const desc of descriptions) {
        datasetDescriptionDtos.push({
            description: desc.description,
            language: desc.languageCode
        });
    }

    const datafiles = await dataset.datafiles;
    for (const dfile of datafiles) {
        datafilesDtos.push({
            id: dfile.id,
            sha256hash: dfile.sha256hash,
            created_by: dfile.createdBy,
            creation_date: dfile.creationDate.toISOString()
        });
    }

    return {
        id: dataset.id,
        code: dataset.code,
        internal_name: dataset.internalName,
        title: datasetTitleDtos,
        description: datasetDescriptionDtos,
        creation_date: dataset.creationDate.toString(),
        created_by: dataset.createdBy,
        modification_date: dataset.lastModified.toString(),
        modified_by: dataset.modifiedBy,
        live: dataset.live,
        datafiles: datafilesDtos,
        csv_link: `/dataset/${dataset.id}/csv`,
        xslx_link: `/dataset/${dataset.id}/xlsx`,
        view_link: `/dataset/${dataset.id}/view`
    };
}
