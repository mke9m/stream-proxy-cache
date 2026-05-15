import { describe, expect, it } from 'vitest';
import { assertHostNotPrivate, hostAllowed, normalizeMediaUrl } from '../src/security/urlPolicy.js';

describe('url policy', () => {
  it('normalizes safe http URLs and strips fragments', () => {
    expect(normalizeMediaUrl('https://EXAMPLE.com/path?a=1#frag').toString()).toBe('https://example.com/path?a=1');
  });

  it('rejects non-http protocols', () => {
    expect(() => normalizeMediaUrl('file:///etc/passwd')).toThrow(/http/);
  });

  it('matches explicit and wildcard allowlist hosts', () => {
    expect(hostAllowed('cdn.example.com', ['*.example.com'])).toBe(true);
    expect(hostAllowed('example.com', ['example.com'])).toBe(true);
    expect(hostAllowed('evil-example.com', ['*.example.com'])).toBe(false);
  });

  it('rejects localhost to prevent SSRF', async () => {
    await expect(assertHostNotPrivate('localhost')).rejects.toThrow(/Localhost/);
  });
});
