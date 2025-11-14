import JSZip from 'jszip';
import { FileImportDto } from '../dtos/file-import';
import { StorageService } from '../interfaces/storage-service';
import { DataLakeFileEntry } from '../interfaces/datalake-file-entry';
import { FileImportType } from '../enums/file-import-type';
import { DatasetRepository } from '../repositories/dataset';

export const addDirectoryToZip = async (
  zip: JSZip,
  datasetFiles: Map<string, FileImportDto>,
  directory: string,
  fileService: StorageService
): Promise<void> => {
  const directoryList = await fileService.listFiles(directory);
  for (const fileEntry of directoryList) {
    let filename: string;
    if ((fileEntry as DataLakeFileEntry).name) {
      const entry = fileEntry as DataLakeFileEntry;
      if (entry.isDirectory) {
        await addDirectoryToZip(zip, datasetFiles, `${directory}/${entry.name}`, fileService);
        continue;
      }
      filename = (fileEntry as DataLakeFileEntry).name;
    } else {
      filename = fileEntry as string;
    }
    const originalFilename = datasetFiles.get(filename)?.filename || filename;
    zip.file(originalFilename, await fileService.loadBuffer(filename, directory));
  }
};

export const collectFiles = async (datasetId: string): Promise<Map<string, FileImportDto>> => {
  const files = new Map<string, FileImportDto>();

  const dataset = await DatasetRepository.getById(datasetId, {
    measure: { lookupTable: true },
    dimensions: { lookupTable: true },
    revisions: { dataTable: true }
  });

  if (dataset.measure && dataset.measure.lookupTable) {
    const fileImport = FileImportDto.fromFileImport(dataset.measure.lookupTable);
    fileImport.type = FileImportType.Measure;
    fileImport.parent_id = dataset.id;
    files.set(dataset.measure.lookupTable.filename, fileImport);
  }

  dataset.dimensions?.forEach((dimension) => {
    if (dimension.lookupTable) {
      const fileImport = FileImportDto.fromFileImport(dimension.lookupTable);
      fileImport.type = FileImportType.Dimension;
      fileImport.parent_id = dimension.id;
      files.set(dimension.lookupTable.filename, fileImport);
    }
  });

  dataset.revisions?.forEach((revision) => {
    if (revision.dataTable) {
      const fileImport = FileImportDto.fromFileImport(revision.dataTable);
      fileImport.type = FileImportType.DataTable;
      fileImport.parent_id = revision.id;
      files.set(revision.dataTable.filename, fileImport);
    }
  });

  return files;
};
