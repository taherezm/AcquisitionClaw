export function normalizeExtractionResult({ validation, extraction, classification }) {
  if (!validation.accepted) {
    return {
      status: 'skipped',
      readyForPipeline: false,
      normalizedDocument: null,
      pipelineContent: null,
      notes: [validation.reason],
    };
  }

  if (extraction.usable && extraction.periods.length > 0) {
    return {
      status: 'ready',
      readyForPipeline: true,
      normalizedDocument: {
        docType: classification.docType,
        periods: extraction.periods,
        data: extraction.data,
      },
      pipelineContent: {
        __parsed: true,
        periods: extraction.periods,
        data: extraction.data,
      },
      notes: [],
    };
  }

  return {
    status: 'pending_mapping',
    readyForPipeline: false,
    normalizedDocument: null,
    pipelineContent: null,
    notes: [
      'Normalization is waiting for extraction to produce enough schema-aligned periods/data for the pipeline.',
      'Review missing fields and mapping confidence before promoting this document into the scoring pipeline.',
    ],
  };
}
