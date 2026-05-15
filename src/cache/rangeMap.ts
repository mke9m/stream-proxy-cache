export type ByteRange = {
  start: number;
  end: number;
};

export type ParsedRange =
  | { kind: 'none' }
  | { kind: 'valid'; start: number; end?: number }
  | { kind: 'suffix'; suffixLength: number }
  | { kind: 'invalid' };

export function parseRangeHeader(header?: string): ParsedRange {
  if (!header) return { kind: 'none' };
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match) return { kind: 'invalid' };
  const [, startRaw, endRaw] = match;
  if (!startRaw && !endRaw) return { kind: 'invalid' };
  if (!startRaw) {
    const suffixLength = Number.parseInt(endRaw, 10);
    return suffixLength > 0 ? { kind: 'suffix', suffixLength } : { kind: 'invalid' };
  }
  const start = Number.parseInt(startRaw, 10);
  const end = endRaw ? Number.parseInt(endRaw, 10) : undefined;
  if (!Number.isSafeInteger(start) || start < 0) return { kind: 'invalid' };
  if (end !== undefined && (!Number.isSafeInteger(end) || end < start)) return { kind: 'invalid' };
  return { kind: 'valid', start, end };
}

export function resolveRange(parsed: ParsedRange, contentLength?: number): ByteRange | undefined {
  if (parsed.kind === 'none') {
    return { start: 0, end: contentLength ? contentLength - 1 : Number.MAX_SAFE_INTEGER };
  }
  if (parsed.kind === 'invalid') return undefined;
  if (parsed.kind === 'suffix') {
    if (contentLength === undefined) return undefined;
    const start = Math.max(0, contentLength - parsed.suffixLength);
    return { start, end: contentLength - 1 };
  }
  if (contentLength !== undefined && parsed.start >= contentLength) return undefined;
  const maxEnd = contentLength === undefined ? Number.MAX_SAFE_INTEGER : contentLength - 1;
  return { start: parsed.start, end: Math.min(parsed.end ?? maxEnd, maxEnd) };
}

export function mergeRanges(ranges: ByteRange[]): ByteRange[] {
  if (ranges.length === 0) return [];
  const sorted = [...ranges].sort((a, b) => a.start - b.start || a.end - b.end);
  const merged: ByteRange[] = [{ ...sorted[0] }];
  for (const range of sorted.slice(1)) {
    const last = merged[merged.length - 1];
    if (range.start <= last.end + 1) {
      last.end = Math.max(last.end, range.end);
    } else {
      merged.push({ ...range });
    }
  }
  return merged;
}

export function subtractRanges(target: ByteRange, cachedRanges: ByteRange[]): ByteRange[] {
  const cached = mergeRanges(cachedRanges);
  let cursor = target.start;
  const missing: ByteRange[] = [];
  for (const range of cached) {
    if (range.end < cursor) continue;
    if (range.start > target.end) break;
    if (range.start > cursor) {
      missing.push({ start: cursor, end: Math.min(range.start - 1, target.end) });
    }
    cursor = Math.max(cursor, range.end + 1);
    if (cursor > target.end) break;
  }
  if (cursor <= target.end) missing.push({ start: cursor, end: target.end });
  return missing;
}

export function rangesToChunkIndexes(range: ByteRange, chunkSize: number): number[] {
  const first = Math.floor(range.start / chunkSize);
  const last = Math.floor(range.end / chunkSize);
  return Array.from({ length: last - first + 1 }, (_, index) => first + index);
}
