export async function ingestFiles(files, companyContext = {}) {
  const formData = new FormData();

  files.forEach((file) => {
    formData.append('files', file, file.name);
  });

  formData.append('companyName', companyContext.companyName || '');
  formData.append('industry', companyContext.industry || '');
  formData.append('ebitdaRange', companyContext.ebitdaRange || '');

  let response;
  try {
    response = await fetch('/api/ingest', {
      method: 'POST',
      body: formData,
    });
  } catch (_error) {
    throw new Error('Unable to reach the ingestion service. Start the backend and try again.');
  }

  const payload = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(payload?.error || 'Backend ingestion failed.');
  }

  return payload;
}

export function buildPipelineFileDescriptors(ingestionResponse) {
  // The scoring pipeline now consumes backend-validated file results directly.
  // Each descriptor carries the structured ingestion payload so classification
  // and extraction can use server-side results before any local fallback logic.
  return (ingestionResponse.files || [])
    .filter((fileResult) => fileResult.validation?.accepted)
    .map((fileResult) => ({
      name: fileResult.file.originalName,
      type: fileResult.file.mimeType,
      size: fileResult.file.size,
      content: {
        __backendIngestion: true,
        backendFileResult: fileResult,
      },
    }));
}
