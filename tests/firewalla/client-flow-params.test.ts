import { FirewallaClient } from '../../src/firewalla/client.js';
import type { FirewallaConfig } from '../../src/types.js';

function createClient(boxId = 'box-123') {
  const config: FirewallaConfig = {
    mspToken: 'test-token',
    mspId: 'test.firewalla.net',
    mspBaseUrl: 'https://test.firewalla.net',
    boxId,
    apiTimeout: 30000,
    rateLimit: 100,
    cacheTtl: 0,
    defaultPageSize: 100,
    maxPageSize: 10000,
    transport: { type: 'stdio', port: 3000, path: '/mcp' },
  };

  return new FirewallaClient(config);
}

describe('FirewallaClient flow API parameter construction', () => {
  it('adds the default box filter with AND instead of concatenating expressions', async () => {
    const client = createClient();
    const calls: Array<{ endpoint: string; params: Record<string, unknown> }> = [];

    (client as any).api = {
      get: jest.fn(async (endpoint: string, options: { params: Record<string, unknown> }) => {
        calls.push({ endpoint, params: options.params });
        return {
          status: 200,
          config: { url: endpoint },
          data: { count: 0, results: [], next_cursor: undefined },
        };
      }),
    };

    await client.getFlowData('ts:100-200', 'device', 'total:desc', 5);

    expect(calls).toHaveLength(1);
    expect(calls[0].endpoint).toBe('/v2/flows');
    expect(calls[0].params).toEqual({
      query: 'ts:100-200 AND gid:box-123',
      groupBy: 'device',
      sortBy: 'total:desc',
      limit: 5,
    });
  });

  it('uses Firewalla camelCase groupBy and sortBy params for searchFlows', async () => {
    const client = createClient();
    const calls: Array<{ endpoint: string; params: Record<string, unknown> }> = [];

    (client as any).api = {
      get: jest.fn(async (endpoint: string, options: { params: Record<string, unknown> }) => {
        calls.push({ endpoint, params: options.params });
        return {
          status: 200,
          config: { url: endpoint },
          data: { count: 0, results: [], next_cursor: undefined },
        };
      }),
    };

    await client.searchFlows({
      query: 'ts:100-200',
      group_by: 'domain,box',
      sort_by: 'total:desc',
      limit: 5,
    });

    expect(calls).toHaveLength(1);
    expect(calls[0].endpoint).toBe('/v2/flows');
    expect(calls[0].params).toEqual({
      query: 'ts:100-200 AND gid:box-123',
      groupBy: 'domain,box',
      sortBy: 'total:desc',
      limit: 5,
    });
  });

  it('preserves raw dotted flow fields and normalizes implicit AND before API calls', async () => {
    const client = createClient();
    const calls: Array<{ endpoint: string; params: Record<string, unknown> }> = [];

    (client as any).api = {
      get: jest.fn(async (endpoint: string, options: { params: Record<string, unknown> }) => {
        calls.push({ endpoint, params: options.params });
        return {
          status: 200,
          config: { url: endpoint },
          data: { count: 0, results: [], next_cursor: undefined },
        };
      }),
    };

    await client.getFlowData('box.id:box-123 source.ip:192.168.1.10', undefined, 'ts:desc', 5);

    expect(calls).toHaveLength(1);
    expect(calls[0].params.query).toBe('box.id:box-123 AND source.ip:192.168.1.10 AND gid:box-123');
  });

  it('translates alias flow fields to Firewalla raw dotted fields before API calls', async () => {
    const client = createClient();
    const calls: Array<{ endpoint: string; params: Record<string, unknown> }> = [];

    (client as any).api = {
      get: jest.fn(async (endpoint: string, options: { params: Record<string, unknown> }) => {
        calls.push({ endpoint, params: options.params });
        return {
          status: 200,
          config: { url: endpoint },
          data: { count: 0, results: [], next_cursor: undefined },
        };
      }),
    };

    await client.searchFlows({
      query: 'source_ip:192.168.1.10 device_ip:192.168.1.10 destination_ip:8.8.8.8 blocked:true',
      limit: 5,
    });

    expect(calls).toHaveLength(1);
    expect(calls[0].params.query).toBe(
      'source.ip:192.168.1.10 AND device.ip:192.168.1.10 AND destination.ip:8.8.8.8 AND block:true AND gid:box-123'
    );
  });

  it('does not translate field-like text inside quoted flow query values', async () => {
    const client = createClient();
    const calls: Array<{ endpoint: string; params: Record<string, unknown> }> = [];

    (client as any).api = {
      get: jest.fn(async (endpoint: string, options: { params: Record<string, unknown> }) => {
        calls.push({ endpoint, params: options.params });
        return {
          status: 200,
          config: { url: endpoint },
          data: { count: 0, results: [], next_cursor: undefined },
        };
      }),
    };

    await client.getFlowData('domain:"foo source_ip:bar blocked:true" source_ip:192.168.1.10', undefined, 'ts:desc', 5);

    expect(calls).toHaveLength(1);
    expect(calls[0].params.query).toBe(
      'domain:"foo source_ip:bar blocked:true" AND source.ip:192.168.1.10 AND gid:box-123'
    );
  });
});
