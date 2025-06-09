import tmp, { TmpNameOptions } from 'tmp';

export const asyncTmpName = async (options: TmpNameOptions): Promise<string> => {
  return new Promise((resolve, reject) => {
    tmp.tmpName(options, (err, path) => {
      if (err) reject(err);
      resolve(path);
    });
  });
};
