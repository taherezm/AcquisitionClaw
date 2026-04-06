import path from 'node:path';

export const SUPPORTED_UPLOAD_EXTENSIONS = new Set(['.csv', '.xlsx', '.pdf']);
export const DEFERRED_UPLOAD_EXTENSIONS = new Set([]);

export function getFileExtension(filename = '') {
  return path.extname(filename || '').toLowerCase();
}

export function toUploadedFileSummary(file) {
  return {
    fieldName: file.fieldname,
    originalName: file.originalname,
    extension: getFileExtension(file.originalname),
    mimeType: file.mimetype || 'application/octet-stream',
    size: file.size || 0,
    encoding: file.encoding || '7bit',
  };
}
