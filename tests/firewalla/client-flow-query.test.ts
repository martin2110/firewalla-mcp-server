import { readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { FirewallaClient } from '../../src/firewalla/client';
import {
  ExportFlowDataHandler,
  GetFlowDataHandler,
} from '../../src/tools/handlers/network';
import type { FirewallaConfig } from '../../src/types';

const BOX_ID = 'box-gid-123';

function createClient(boxId: string | undefined = BOX_ID): FirewallaClient {
  const config: FirewallaConfig = {
    mspToken: 'test-token-redacted',
    mspId: 'test-msp',
    mspBaseUrl: 'https://firewalla.test',
    boxId,
    apiTimeout: 5000,
    rateLimit: 100,
    cacheTtl: 0,
    defaultPageSize: 100,
    maxPageSize: 10000,
    transport: {
      type: 'stdio',
      port: 3000,
      path: '/mcp',
    },
  };

  return new FirewallaClient(config);
}

function mockFlowRequest(client: FirewallaClient): jest.SpyInstance {
  return jest.spyOn(client as any, 'request').mockResolvedValue({
    count: 1,
    results: [
      {
        id: 'flow-1',
        ts: 1710000000,
        srcIP: '192.168.1.50',
        dstIP: '192.168.1.1',
        device: {
          id: 'device-1',
          ip: '192.168.1.50',
          name: 'Laptop',
        },
      },
    ],
  });
}

describe('FirewallaClient getFlowData flow query construction', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('uses Firewalla-compatible box.id query syntax for an empty flow query and returns the final query', async () => {
    const expectedQuery = `box.id:${BOX_ID}`;
    const client = createClient();
    const requestSpy = mockFlowRequest(client);

    const response = await client.getFlowData();

    expect(requestSpy).toHaveBeenCalledWith(
      'GET',
      '/v2/flows',
      expect.objectContaining({ query: expectedQuery })
    );
    expect(response.count).toBe(1);
    expect(response.results).toHaveLength(1);
    expect(response.final_query).toBe(expectedQuery);
  });

  it('prepends the box.id filter to an existing flow query with implicit-AND spacing and returns the final query', async () => {
    const userQuery = 'source.ip:192.168.1.50';
    const expectedQuery = `box.id:${BOX_ID} ${userQuery}`;
    const client = createClient();
    const requestSpy = mockFlowRequest(client);

    const response = await client.getFlowData(userQuery);

    expect(requestSpy).toHaveBeenCalledWith(
      'GET',
      '/v2/flows',
      expect.objectContaining({ query: expectedQuery })
    );
    expect(response.final_query).toBe(expectedQuery);
  });

  it('does not duplicate the box.id filter when the existing flow query already contains the exact filter', async () => {
    const userQuery = `box.id:${BOX_ID} source.ip:192.168.1.50`;
    const client = createClient();
    const requestSpy = mockFlowRequest(client);

    const response = await client.getFlowData(userQuery);

    expect(requestSpy).toHaveBeenCalledWith(
      'GET',
      '/v2/flows',
      expect.objectContaining({ query: userQuery })
    );
    expect(response.final_query).toBe(userQuery);
  });

  it('does not treat a prefix-matching box.id value as the configured box filter', async () => {
    const userQuery = `box.id:${BOX_ID}4 source.ip:192.168.1.50`;
    const expectedQuery = `box.id:${BOX_ID} ${userQuery}`;
    const client = createClient();
    const requestSpy = mockFlowRequest(client);

    const response = await client.getFlowData(userQuery);

    expect(requestSpy).toHaveBeenCalledWith(
      'GET',
      '/v2/flows',
      expect.objectContaining({ query: expectedQuery })
    );
    expect(response.final_query).toBe(expectedQuery);
  });

  it('leaves the query unchanged when no default box ID is configured', async () => {
    const userQuery = 'source.ip:192.168.1.50';
    const client = createClient('');
    const requestSpy = mockFlowRequest(client);

    const response = await client.getFlowData(userQuery);

    expect(requestSpy).toHaveBeenCalledWith(
      'GET',
      '/v2/flows',
      expect.objectContaining({ query: userQuery })
    );
    expect(response.final_query).toBe(userQuery);
  });

  it('stops pagination safely when Firewalla repeats the requested cursor', async () => {
    const repeatedCursor = 'b2Zmc2V0IDUwMA==';
    const client = createClient();
    jest.spyOn(client as any, 'request').mockResolvedValue({
      count: 1,
      results: [
        {
          ts: 1710000000,
          srcIP: '192.168.1.50',
          dstIP: '192.168.1.1',
          device: { id: 'device-1', ip: '192.168.1.50', name: 'Laptop' },
        },
      ],
      next_cursor: repeatedCursor,
    });

    const response = await client.getFlowData(
      undefined,
      undefined,
      'ts:desc',
      500,
      repeatedCursor
    );

    expect(response.next_cursor).toBeUndefined();
    expect(response.has_more).toBe(false);
    expect(response.pages_fetched).toBe(1);
    expect(response.stopped_reason).toBe('repeated_cursor');
    expect(response.repeated_cursor).toBe(repeatedCursor);
  });
});

describe('GetFlowDataHandler final query metadata', () => {
  it('reports the actual final query returned by FirewallaClient in the non-streaming tool response metadata', async () => {
    const finalQuery = `box.id:${BOX_ID} source.ip:192.168.1.50`;
    const handler = new GetFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn().mockResolvedValue({
        count: 0,
        results: [],
        final_query: finalQuery,
      }),
    } as any;

    const response = await handler.execute(
      { query: 'source.ip:192.168.1.50', limit: 50 },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(firewalla.getFlowData).toHaveBeenCalledWith(
      'source.ip:192.168.1.50',
      undefined,
      undefined,
      50,
      undefined
    );
    expect(payload.data.query_parameters.query).toBe(finalQuery);
    expect(payload.data.query_parameters.query).not.toContain(
      'test-token-redacted'
    );
  });

  it('reports the actual final query in the default streaming tool response metadata', async () => {
    const finalQuery = `box.id:${BOX_ID} source.ip:192.168.1.50`;
    const handler = new GetFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn().mockResolvedValue({
        count: 0,
        results: [],
        final_query: finalQuery,
      }),
    } as any;

    const response = await handler.execute(
      { query: 'source.ip:192.168.1.50' },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(payload.streaming).toBe(true);
    expect(payload.metadata.query_parameters.query).toBe(finalQuery);
    expect(payload.metadata.query_parameters.query).not.toContain(
      'test-token-redacted'
    );
  });

  it('reports safe pagination metadata in non-streaming flow responses', async () => {
    const handler = new GetFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn().mockResolvedValue({
        count: 0,
        results: [],
        next_cursor: undefined,
        has_more: false,
        pages_fetched: 1,
        stopped_reason: 'repeated_cursor',
        repeated_cursor: 'b2Zmc2V0IDUwMA==',
      }),
    } as any;

    const response = await handler.execute(
      { limit: 50, cursor: 'b2Zmc2V0IDUwMA==' },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(payload.data.pagination.pages_fetched).toBe(1);
    expect(payload.data.pagination.has_more).toBe(false);
    expect(payload.data.pagination.next_cursor).toBeNull();
    expect(payload.data.pagination.stopped_reason).toBe('repeated_cursor');
    expect(payload.data.pagination.repeated_cursor).toBe('b2Zmc2V0IDUwMA==');
  });

  it('ends a streaming session when a page repeats the previous cursor and uses safe page sizes', async () => {
    const repeatedCursor = 'b2Zmc2V0IDUwMA==';
    const handler = new GetFlowDataHandler();
    const firewalla = {
      getFlowData: jest
        .fn()
        .mockImplementation(
          async (
            _query,
            _groupBy,
            _sortBy,
            _limit,
            cursor: string | undefined
          ) => ({
            count: 1,
            results: [
              {
                ts: 1710000000,
                protocol: 'tcp',
                download: 1,
                upload: 1,
                count: 1,
                device: { id: 'device-1', ip: '192.168.1.50', name: 'Laptop' },
              },
            ],
            next_cursor: repeatedCursor,
            has_more: cursor !== repeatedCursor,
            pages_fetched: cursor === repeatedCursor ? 2 : 1,
            stopped_reason:
              cursor === repeatedCursor ? 'repeated_cursor' : undefined,
            repeated_cursor:
              cursor === repeatedCursor ? repeatedCursor : undefined,
          })
        ),
    } as any;

    const firstResponse = await handler.execute({ limit: 500 }, firewalla);
    const firstPayload = JSON.parse(firstResponse.content[0].text);

    expect(firstPayload.streaming).toBe(true);
    expect(firstPayload.isFinalChunk).toBe(false);
    expect(firstPayload.nextContinuationToken).toBe(repeatedCursor);

    const secondResponse = await handler.execute(
      { limit: 500, streaming_session_id: firstPayload.sessionId },
      firewalla
    );
    const secondPayload = JSON.parse(secondResponse.content[0].text);

    expect(firewalla.getFlowData).toHaveBeenNthCalledWith(
      1,
      undefined,
      undefined,
      undefined,
      50,
      undefined
    );
    expect(firewalla.getFlowData).toHaveBeenNthCalledWith(
      2,
      undefined,
      undefined,
      undefined,
      50,
      repeatedCursor
    );
    expect(secondPayload.isFinalChunk).toBe(true);
    expect(secondPayload.nextContinuationToken).toBeNull();
    expect(secondPayload.metadata.pages_fetched).toBe(2);
    expect(secondPayload.metadata.has_more).toBe(false);
    expect(secondPayload.metadata.next_cursor).toBeNull();
    expect(secondPayload.metadata.stopped_reason).toBe('repeated_cursor');
  });
});

describe('ExportFlowDataHandler', () => {
  let outputDir: string;

  beforeEach(() => {
    outputDir = `test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  });

  afterEach(async () => {
    await rm(join(process.cwd(), 'firewalla-flow-exports', outputDir), {
      recursive: true,
      force: true,
    });
    await rm(join(process.cwd(), 'firewalla-flow-exports', 'tmp-evil'), {
      recursive: true,
      force: true,
    });
    jest.restoreAllMocks();
  });

  it('paginates flows to raw JSON and CSV artifacts and returns a compact summary without secrets', async () => {
    const handler = new ExportFlowDataHandler();
    const firewalla = {
      getFlowData: jest
        .fn()
        .mockResolvedValueOnce({
          count: 2,
          results: [
            {
              ts: 1710000000,
              protocol: 'tcp',
              download: 100,
              upload: 50,
              block: false,
              device: { id: 'device-1', ip: '192.168.1.50', name: 'Laptop' },
              destination: { ip: '1.1.1.1', name: 'one.example' },
            },
            {
              ts: 1710000060,
              protocol: 'udp',
              download: 10,
              upload: 5,
              block: true,
              device: { id: 'device-2', ip: '192.168.1.51', name: 'Tablet' },
              destination: { ip: '2.2.2.2', name: 'two.example' },
            },
          ],
          next_cursor: 'cursor-2',
          has_more: true,
          final_query: 'box.id:box-gid-123 category:social',
          pages_fetched: 1,
          requested_limit: 2,
          applied_limit: 2,
        })
        .mockResolvedValueOnce({
          count: 1,
          results: [
            {
              ts: 1710000120,
              protocol: 'tcp',
              download: 200,
              upload: 25,
              block: false,
              device: { id: 'device-3', ip: '192.168.1.52', name: 'Desktop' },
              destination: { ip: '1.1.1.1', name: 'one.example' },
            },
          ],
          has_more: false,
          final_query: 'box.id:box-gid-123 category:social',
          pages_fetched: 1,
          requested_limit: 2,
          applied_limit: 2,
        }),
    } as any;

    const response = await handler.execute(
      {
        query: 'category:social',
        page_size: 2,
        max_pages: 5,
        output_dir: outputDir,
        export_prefix: 'weekly social audit',
      },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(firewalla.getFlowData).toHaveBeenNthCalledWith(
      1,
      'category:social',
      undefined,
      undefined,
      2,
      undefined
    );
    expect(firewalla.getFlowData).toHaveBeenNthCalledWith(
      2,
      'category:social',
      undefined,
      undefined,
      2,
      'cursor-2'
    );
    expect(payload.data.flows).toBeUndefined();
    expect(payload.data.row_count).toBe(3);
    expect(payload.data.pages_fetched).toBe(2);
    expect(payload.data.stopped_reason).toBe('no_more_results');
    expect(payload.data.total_bytes).toBe(390);
    expect(payload.data.blocked_count).toBe(1);
    expect(payload.data.unique_destinations).toBe(2);
    expect(payload.data.observed_time_range).toEqual({
      start: '2024-03-09T16:00:00.000Z',
      end: '2024-03-09T16:02:00.000Z',
    });
    expect(payload.data.artifacts.raw_json_path).toMatch(
      /weekly-social-audit.*\.json$/
    );
    expect(payload.data.artifacts.csv_path).toMatch(
      /weekly-social-audit.*\.csv$/
    );
    expect(payload.data.query_parameters.query).toBe(
      'box.id:box-gid-123 category:social'
    );

    const rawJson = await readFile(
      payload.data.artifacts.raw_json_path,
      'utf8'
    );
    const csv = await readFile(payload.data.artifacts.csv_path, 'utf8');
    expect(JSON.parse(rawJson).flows).toHaveLength(3);
    expect(csv).toContain('timestamp,source_ip,destination_ip');
    expect(rawJson).not.toContain('test-token-redacted');
    expect(csv).not.toContain('test-token-redacted');
  });

  it('stops safely when a repeated pagination cursor is observed', async () => {
    const handler = new ExportFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn().mockResolvedValue({
        count: 1,
        results: [
          {
            ts: 1710000000,
            download: 1,
            upload: 2,
            destination: { ip: '1.1.1.1', name: 'one.example' },
          },
        ],
        next_cursor: 'same-cursor',
        has_more: true,
        final_query: 'box.id:box-gid-123',
      }),
    } as any;

    const response = await handler.execute(
      {
        page_size: 1,
        max_pages: 3,
        cursor: 'same-cursor',
        output_dir: outputDir,
      },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(firewalla.getFlowData).toHaveBeenCalledTimes(1);
    expect(payload.data.row_count).toBe(1);
    expect(payload.data.stopped_reason).toBe('repeated_cursor');
    expect(payload.data.repeated_cursor).toBe('same-cursor');
    expect(payload.data.has_more).toBe(false);
  });

  it('neutralizes spreadsheet formula cells and keeps absolute output_dir under the safe export base', async () => {
    const handler = new ExportFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn().mockResolvedValue({
        count: 1,
        results: [
          {
            ts: 1710000000,
            download: 1,
            upload: 2,
            device: {
              id: 'device-1',
              ip: '192.168.1.50',
              name: '=HYPERLINK("https://evil.example")',
            },
            destination: { ip: '1.1.1.1', name: '+SUM(1,1)' },
          },
        ],
        has_more: false,
        final_query: 'box.id:box-gid-123',
      }),
    } as any;

    const response = await handler.execute(
      {
        page_size: 1,
        max_pages: 1,
        output_dir: '/tmp/evil',
      },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);
    const csv = await readFile(payload.data.artifacts.csv_path, 'utf8');

    expect(payload.data.artifacts.csv_path).toContain(
      join(process.cwd(), 'firewalla-flow-exports', 'tmp-evil')
    );
    expect(csv).toContain("'+SUM(1,1)");
    expect(csv).toContain("'=");
  });

  it('rejects parent-directory output_dir values after sanitization', async () => {
    const handler = new ExportFlowDataHandler();
    const firewalla = {
      getFlowData: jest.fn(),
    } as any;

    const response = await handler.execute(
      {
        page_size: 1,
        max_pages: 1,
        output_dir: '..',
      },
      firewalla
    );
    const payload = JSON.parse(response.content[0].text);

    expect(response.isError).toBe(true);
    expect(payload.message).toContain('Invalid output_dir');
    expect(firewalla.getFlowData).not.toHaveBeenCalled();
  });
});
