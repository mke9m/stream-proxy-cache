export type ProxyStats = {
  cacheHits: number;
  cacheMisses: number;
  bytesServedFromCache: number;
  bytesFetchedFromUpstream: number;
  activeStreams: number;
};

export const proxyStats: ProxyStats = {
  cacheHits: 0,
  cacheMisses: 0,
  bytesServedFromCache: 0,
  bytesFetchedFromUpstream: 0,
  activeStreams: 0
};
