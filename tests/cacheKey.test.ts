import { describe, expect, it } from 'vitest';
import { cacheKeyForUrl } from '../src/cache/fileCache.js';

describe('cache key generation', () => {
  it('is stable and hides the original URL', () => {
    const first = cacheKeyForUrl('https://example.com/video.mp4?token=secret');
    const second = cacheKeyForUrl('https://example.com/video.mp4?token=secret');
    expect(first).toEqual(second);
    expect(first.cacheKey).not.toContain('secret');
    expect(first.urlHash).toHaveLength(64);
  });
});
