import { lookup } from 'node:dns/promises';
import ipaddr from 'ipaddr.js';

export type UrlPolicy = {
  allowlistHosts: string[];
  allowPrivateUpstreamsForTesting?: boolean;
};

const blockedRanges = new Set([
  'unspecified',
  'broadcast',
  'multicast',
  'linkLocal',
  'loopback',
  'private',
  'reserved',
  'uniqueLocal',
  'ipv4Mapped',
  'carrierGradeNat'
]);

export function normalizeMediaUrl(rawUrl: string): URL {
  const url = new URL(rawUrl);
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new Error('Only http and https URLs are allowed');
  }
  url.hash = '';
  url.hostname = url.hostname.toLowerCase();
  return url;
}

export async function assertUrlAllowed(rawUrl: string, policy: UrlPolicy): Promise<URL> {
  const url = normalizeMediaUrl(rawUrl);
  if (policy.allowlistHosts.length > 0 && !hostAllowed(url.hostname, policy.allowlistHosts)) {
    throw new Error('Upstream host is not in allowlist');
  }
  if (!policy.allowPrivateUpstreamsForTesting) {
    await assertHostNotPrivate(url.hostname);
  }
  return url;
}

export function hostAllowed(hostname: string, allowlist: string[]): boolean {
  const host = hostname.toLowerCase();
  return allowlist.some((allowed) => {
    const candidate = allowed.toLowerCase();
    return host === candidate || (candidate.startsWith('*.') && host.endsWith(candidate.slice(1)));
  });
}

export async function assertHostNotPrivate(hostname: string): Promise<void> {
  if (hostname === 'localhost' || hostname.endsWith('.localhost')) {
    throw new Error('Localhost URLs are not allowed');
  }
  const addresses = await lookup(hostname, { all: true, verbatim: false });
  if (addresses.length === 0) throw new Error('Upstream host did not resolve');
  for (const result of addresses) {
    const parsed = ipaddr.parse(result.address);
    const range = parsed.range();
    if (blockedRanges.has(range)) {
      throw new Error(`Blocked private or internal upstream address: ${range}`);
    }
  }
}
