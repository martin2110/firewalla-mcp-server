import { FirewallaClient } from '../../src/firewalla/client';
import { GetFlowDataHandler } from '../../src/tools/handlers/network';
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
    expect(payload.data.query_parameters.query).not.toContain('test-token-redacted');
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
});
