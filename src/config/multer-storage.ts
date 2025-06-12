import multer from 'multer';
import path from 'node:path';
import { randomBytes } from 'node:crypto';

export const multerStorageDir = '/tmp/multer-storage';

export const storageConfig = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, multerStorageDir);
  },
  filename: function (req, file, cb) {
    randomBytes(16, (err, buf) => {
      if (err) {
        return cb(err, '');
      }
      const uniqueSuffix = buf.toString('hex');
      // Preserve the original file extension, converting it to lowercase
      const extension = path.extname(file.originalname).toLowerCase();
      cb(null, uniqueSuffix + extension);
    });
  }
});
