import { describe, expect, it } from 'vitest';
import { mergeRanges, parseRangeHeader, rangesToChunkIndexes, resolveRange, subtractRanges } from '../src/cache/rangeMap.js';

describe('range parsing', () => {
  it('parses open-ended and fixed byte ranges', () => {
    expect(parseRangeHeader('bytes=0-')).toEqual({ kind: 'valid', start: 0, end: undefined });
    expect(parseRangeHeader('bytes=10-20')).toEqual({ kind: 'valid', start: 10, end: 20 });
  });

  it('parses suffix ranges', () => {
    expect(parseRangeHeader('bytes=-500')).toEqual({ kind: 'suffix', suffixLength: 500 });
  });

  it('rejects malformed ranges', () => {
    expect(parseRangeHeader('items=0-1')).toEqual({ kind: 'invalid' });
    expect(parseRangeHeader('bytes=9-1')).toEqual({ kind: 'invalid' });
  });

  it('resolves ranges against content length', () => {
    expect(resolveRange(parseRangeHeader('bytes=5-'), 10)).toEqual({ start: 5, end: 9 });
    expect(resolveRange(parseRangeHeader('bytes=-3'), 10)).toEqual({ start: 7, end: 9 });
    expect(resolveRange(parseRangeHeader('bytes=99-'), 10)).toBeUndefined();
  });
});

describe('range merging and subtraction', () => {
  it('merges overlapping and adjacent ranges', () => {
    expect(mergeRanges([{ start: 10, end: 12 }, { start: 0, end: 4 }, { start: 5, end: 8 }])).toEqual([
      { start: 0, end: 8 },
      { start: 10, end: 12 }
    ]);
  });

  it('finds missing gaps inside a target range', () => {
    expect(subtractRanges({ start: 0, end: 19 }, [{ start: 0, end: 4 }, { start: 10, end: 14 }])).toEqual([
      { start: 5, end: 9 },
      { start: 15, end: 19 }
    ]);
  });

  it('maps requested ranges to chunk bitmap indexes', () => {
    expect(rangesToChunkIndexes({ start: 0, end: 4095 }, 4096)).toEqual([0]);
    expect(rangesToChunkIndexes({ start: 4090, end: 8199 }, 4096)).toEqual([0, 1, 2]);
  });
});
