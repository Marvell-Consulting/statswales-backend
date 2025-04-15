import { getFileService } from '../utils/get-file-service';
import { Revision } from '../entities/dataset/revision';

export const getTableRowsNoFilterNoSort = async (
  datasetId: string,
  revision: Revision,
  lang: string,
  start?: 0,
  end?: 100
) => {
  const { parquetMetadata, parquetReadObjects } = await import('hyparquet');
  const fileService = getFileService();
  const stream = await fileService.loadStream(`${revision.id}_${lang}.parquet`, datasetId);

  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  const buffer = Buffer.concat(chunks);
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);

  const asyncBuffer = {
    byteLength: arrayBuffer.byteLength,
    slice: async (start: number | undefined, end: number | undefined) => arrayBuffer.slice(start, end)
  };

  const metadata = parquetMetadata(arrayBuffer);
  const rows = await parquetReadObjects({ file: asyncBuffer, rowStart: start, rowEnd: end });

  return { metadata, rows };
};
