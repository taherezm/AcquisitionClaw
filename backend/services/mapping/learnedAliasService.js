export function mergeLearnedAliases(baseAliasMap = {}, docType, learnedAliasRules = []) {
  const nextAliasMap = Object.fromEntries(
    Object.entries(baseAliasMap).map(([fieldName, aliases]) => [fieldName, [...aliases]]),
  );

  for (const rule of normalizeLearnedAliasRules(learnedAliasRules)) {
    if (rule.docType !== docType) continue;
    if (!nextAliasMap[rule.fieldName]) {
      nextAliasMap[rule.fieldName] = [];
    }
    if (!nextAliasMap[rule.fieldName].includes(rule.alias)) {
      nextAliasMap[rule.fieldName].push(rule.alias);
    }
  }

  return nextAliasMap;
}

export function normalizeLearnedAliasRules(rules = []) {
  if (!Array.isArray(rules)) return [];

  return rules
    .map((rule) => ({
      docType: rule?.docType || '',
      fieldName: rule?.fieldName || '',
      alias: String(rule?.alias || rule?.rowLabel || '').trim(),
      source: rule?.source || 'manual_override',
    }))
    .filter((rule) => rule.docType && rule.fieldName && rule.alias);
}
