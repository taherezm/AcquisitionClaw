import multer from 'multer';

const storage = multer.memoryStorage();

export const uploadFiles = multer({
  storage,
  limits: {
    files: 20,
    fileSize: 25 * 1024 * 1024,
  },
});
