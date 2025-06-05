import { access } from 'fs/promises';

export const asyncFileExists = async (filePath: string): Promise<boolean> => {
  return access(filePath).then(
    () => true,
    () => false
  );
};
