// similar to express.Multer.File interface
export interface TempFile {
  path: string;
  originalname: string;
  mimetype: string;
  encoding?: string;
  size?: number;
}
