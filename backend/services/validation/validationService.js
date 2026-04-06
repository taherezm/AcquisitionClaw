import {
  DEFERRED_UPLOAD_EXTENSIONS,
  SUPPORTED_UPLOAD_EXTENSIONS,
  getFileExtension,
} from '../../utils/fileUtils.js';

export function validateUploadedFile(file) {
  const extension = getFileExtension(file?.originalname);

  if (!file) {
    return {
      accepted: false,
      status: 'rejected',
      reason: 'Upload payload is missing a file object.',
      supportedExtensions: [...SUPPORTED_UPLOAD_EXTENSIONS],
    };
  }

  if (SUPPORTED_UPLOAD_EXTENSIONS.has(extension)) {
    return {
      accepted: true,
      status: 'accepted',
      reason: null,
      supportedExtensions: [...SUPPORTED_UPLOAD_EXTENSIONS],
    };
  }

  if (DEFERRED_UPLOAD_EXTENSIONS.has(extension)) {
    return {
      accepted: false,
      status: 'deferred',
      reason: `Parsing for ${extension} is not enabled in this build yet.`,
      supportedExtensions: [...SUPPORTED_UPLOAD_EXTENSIONS],
    };
  }

  return {
    accepted: false,
    status: 'rejected',
    reason: `Unsupported file type "${extension || 'unknown'}". This build accepts ${[...SUPPORTED_UPLOAD_EXTENSIONS].join(', ')} files.`,
    supportedExtensions: [...SUPPORTED_UPLOAD_EXTENSIONS],
  };
}

export function summarizeValidationResults(results) {
  const acceptedFiles = results.filter((result) => result.validation.accepted).length;
  const deferredFiles = results.filter((result) => result.validation.status === 'deferred').length;
  const rejectedFiles = results.length - acceptedFiles - deferredFiles;

  return {
    totalFiles: results.length,
    acceptedFiles,
    deferredFiles,
    rejectedFiles,
    supportedExtensions: [...SUPPORTED_UPLOAD_EXTENSIONS],
  };
}
