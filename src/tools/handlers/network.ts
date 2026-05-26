/**
 * Network monitoring and analysis tool handlers
 */

import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve, sep } from 'node:path';
import { BaseToolHandler, type ToolArgs, type ToolResponse } from './base.js';
import type { FirewallaClient } from '../../firewalla/client.js';
import {
  ParameterValidator,
  SafeAccess,
  ErrorType,
} from '../../validation/error-handler.js';
import {
  unixToISOStringOrNow,
  safeUnixToISOString,
} from '../../utils/timestamp.js';
import {
  normalizeUnknownFields,
  sanitizeFieldValue,
  batchNormalize,
  sanitizeByteCount,
} from '../../utils/data-normalizer.js';
import { ResponseStandardizer } from '../../utils/response-standardizer.js';
import type { PaginationMetadata } from '../../types.js';
import { getLimitValidationConfig } from '../../config/limits.js';
import {
  withToolTimeout,
  TimeoutError,
  createTimeoutErrorResponse,
} from '../../utils/timeout-manager.js';
import {
  StreamingManager,
  shouldUseStreaming,
  createStreamingResponse,
  type StreamingOperation,
} from '../../utils/streaming-manager.js';

interface FlowObservedTimeRange {
  oldest: string;
  newest: string;
  start: string;
  end: string;
}

function getObservedTimeRange(flows: unknown): FlowObservedTimeRange | null {
  if (!Array.isArray(flows) || flows.length === 0) {
    return null;
  }

  const timestamps = flows
    .map(flow => optionalUnixTimestamp(SafeAccess.getNestedValue(flow, 'ts')))
    .filter((timestamp): timestamp is number => timestamp !== undefined);

  if (timestamps.length === 0) {
    return null;
  }

  const oldestTs = Math.min(...timestamps);
  const newestTs = Math.max(...timestamps);
  const oldest = safeUnixToISOString(oldestTs, '');
  const newest = safeUnixToISOString(newestTs, '');

  if (!oldest || !newest) {
    return null;
  }

  return {
    oldest,
    newest,
    start: oldest,
    end: newest,
  };
}

function getFlowObservationMetadata(
  response: any,
  query: unknown,
  hasMore: boolean
): Record<string, unknown> {
  const pagesFetched = response.pages_fetched ?? 1;
  const stoppedReason =
    response.stopped_reason ?? (hasMore ? 'page_limit' : 'no_more_results');

  return {
    observed_time_range: getObservedTimeRange(response.results),
    total_records_fetched: Array.isArray(response.results)
      ? response.results.length
      : 0,
    pages_fetched: pagesFetched,
    page_count: pagesFetched,
    has_more: hasMore,
    stopped_reason: stoppedReason,
    query,
  };
}

export class GetFlowDataHandler extends BaseToolHandler {
  name = 'get_flow_data';
  description =
    'Query network traffic flows with pagination. Data is cached for 15 seconds for performance. Use force_refresh=true to bypass cache for real-time data.';
  category = 'network' as const;
  private readonly streamingManager = StreamingManager.forTool(this.name);

  constructor() {
    super({
      enableGeoEnrichment: true,
      enableFieldNormalization: true,
      additionalMeta: {
        data_source: 'flows',
        entity_type: 'network_flows',
        supports_geographic_enrichment: true,
        supports_field_normalization: true,
        supports_streaming: true,
        supports_pagination: true,
        standardization_version: '2.0.0',
      },
    });
  }

  async execute(
    rawArgs: unknown,
    firewalla: FirewallaClient
  ): Promise<ToolResponse> {
    // Early parameter sanitization to prevent null/undefined errors
    const sanitizationResult = this.sanitizeParameters(rawArgs);

    if ('errorResponse' in sanitizationResult) {
      return sanitizationResult.errorResponse;
    }

    const args = sanitizationResult.sanitizedArgs;
    const startTime = Date.now();

    try {
      // Parameter validation
      const limitValidation = ParameterValidator.validateNumber(
        args?.limit,
        'limit',
        {
          required: false,
          defaultValue: 200,
          ...getLimitValidationConfig(this.name),
        }
      );

      if (!limitValidation.isValid) {
        return this.createErrorResponse(
          'Parameter validation failed',
          ErrorType.VALIDATION_ERROR,
          undefined,
          limitValidation.errors
        );
      }

      const query = args?.query;
      const groupBy = args?.groupBy;
      const sortBy = args?.sortBy;
      const limit = limitValidation.sanitizedValue! as number;
      const cursor = args?.cursor;

      // Check if streaming is requested or should be automatically enabled
      const enableStreaming =
        Boolean(args?.stream) || shouldUseStreaming(this.name, limit);
      const streamingSessionId = args?.streaming_session_id as
        | string
        | undefined;

      // Validate individual date parameters before building query
      const startTimeArg = args?.start_time as string | undefined;
      const endTime = args?.end_time as string | undefined;
      let finalQuery = query;

      // Validate start_time if provided
      if (startTimeArg !== undefined) {
        const startTimeValidation = ParameterValidator.validateDateFormat(
          startTimeArg,
          'start_time',
          false
        );
        if (!startTimeValidation.isValid) {
          return this.createErrorResponse(
            'Invalid start_time format',
            ErrorType.VALIDATION_ERROR,
            {
              provided_value: startTimeArg,
              documentation:
                'See /docs/query-syntax-guide.md for time range examples',
            },
            startTimeValidation.errors
          );
        }
      }

      // Validate end_time if provided
      if (endTime !== undefined) {
        const endTimeValidation = ParameterValidator.validateDateFormat(
          endTime,
          'end_time',
          false
        );
        if (!endTimeValidation.isValid) {
          return this.createErrorResponse(
            'Invalid end_time format',
            ErrorType.VALIDATION_ERROR,
            {
              provided_value: endTime,
              documentation:
                'See /docs/query-syntax-guide.md for time range examples',
            },
            endTimeValidation.errors
          );
        }
      }

      // Validate cursor format if provided
      if (cursor !== undefined) {
        const cursorValidation = ParameterValidator.validateCursor(
          cursor,
          'cursor'
        );
        if (!cursorValidation.isValid) {
          return this.createErrorResponse(
            'Invalid cursor format',
            ErrorType.VALIDATION_ERROR,
            {
              provided_value: cursor,
              documentation:
                'Cursors should be obtained from previous response next_cursor field',
            },
            cursorValidation.errors
          );
        }
      }

      // Build time range query if both dates are provided and valid
      if (startTimeArg && endTime) {
        const startDate = new Date(startTimeArg);
        const endDate = new Date(endTime);

        // Validate time range order (dates are already validated for format above)
        if (startDate >= endDate) {
          return this.createErrorResponse(
            'Invalid time range order',
            ErrorType.VALIDATION_ERROR,
            {
              details: 'Start time must be before end time',
              received: {
                start_time: startTimeArg,
                end_time: endTime,
                parsed_start: startDate.toISOString(),
                parsed_end: endDate.toISOString(),
              },
              time_difference: `Start is ${Math.abs(startDate.getTime() - endDate.getTime()) / 1000} seconds after end`,
            },
            [
              'Ensure start_time is chronologically before end_time',
              'Check timezone handling - times may be in different zones',
              'Verify date format includes correct year/month/day values',
              'For recent data, try: start_time: "2024-01-01T00:00:00Z", end_time: "2024-01-02T00:00:00Z"',
            ]
          );
        }

        const startTs = Math.floor(startDate.getTime() / 1000);
        const endTs = Math.floor(endDate.getTime() / 1000);
        const timeQuery = `ts:${startTs}-${endTs}`;
        finalQuery = query ? `(${query}) AND ${timeQuery}` : timeQuery;
      }

      // Handle streaming mode if enabled
      if (enableStreaming) {
        const { streamingManager } = this;

        // Define the streaming operation
        const streamingOperation: StreamingOperation = async params => {
          const response = await withToolTimeout(
            async () =>
              firewalla.getFlowData(
                finalQuery,
                groupBy,
                sortBy,
                params.limit || 100,
                params.cursor
              ),
            this.name
          );

          // Process flows for this chunk
          const processedFlows = SafeAccess.safeArrayMap(
            response.results,
            (flow: any) => ({
              timestamp: unixToISOStringOrNow(flow.ts),
              source_ip: SafeAccess.getNestedValue(
                flow,
                'source.ip',
                SafeAccess.getNestedValue(flow, 'device.ip', 'unknown')
              ),
              destination_ip: SafeAccess.getNestedValue(
                flow,
                'destination.ip',
                'unknown'
              ),
              protocol: SafeAccess.getNestedValue(flow, 'protocol', 'unknown'),
              bytes:
                (SafeAccess.getNestedValue(flow, 'download', 0) as number) +
                (SafeAccess.getNestedValue(flow, 'upload', 0) as number),
              download: SafeAccess.getNestedValue(flow, 'download', 0),
              upload: SafeAccess.getNestedValue(flow, 'upload', 0),
              packets: SafeAccess.getNestedValue(flow, 'count', 0),
              duration: SafeAccess.getNestedValue(flow, 'duration', 0),
              direction: SafeAccess.getNestedValue(
                flow,
                'direction',
                'unknown'
              ),
              blocked: SafeAccess.getNestedValue(flow, 'block', false),
              block_type: SafeAccess.getNestedValue(flow, 'blockType', null),
              device: SafeAccess.getNestedValue(flow, 'device', {}),
              source: SafeAccess.getNestedValue(flow, 'source', {}),
              destination: SafeAccess.getNestedValue(flow, 'destination', {}),
              region: SafeAccess.getNestedValue(flow, 'region', null),
              category: SafeAccess.getNestedValue(flow, 'category', null),
            })
          );

          const actualFinalQuery = response.final_query || finalQuery;
          const hasMore = response.has_more ?? !!response.next_cursor;
          const nextCursor = hasMore ? response.next_cursor : null;
          const observationMetadata = getFlowObservationMetadata(
            response,
            actualFinalQuery,
            hasMore
          );

          return {
            data: processedFlows,
            hasMore,
            nextCursor,
            total: (response as any).total_count,
            metadata: {
              query_parameters: {
                query: actualFinalQuery,
                groupBy,
                sortBy,
                start_time: startTimeArg,
                end_time: endTime,
              },
              ...observationMetadata,
              pages_fetched: response.pages_fetched ?? 1,
              has_more: hasMore,
              next_cursor: nextCursor,
              stopped_reason: observationMetadata.stopped_reason,
              repeated_cursor: response.repeated_cursor,
              requested_limit: response.requested_limit,
              applied_limit: response.applied_limit,
            },
          };
        };

        if (streamingSessionId) {
          // Continue existing streaming session
          const chunk = await streamingManager.continueStreaming(
            streamingSessionId,
            streamingOperation
          );

          if (!chunk) {
            return this.createErrorResponse(
              'Failed to continue streaming session',
              ErrorType.API_ERROR
            );
          }

          return createStreamingResponse(chunk);
        }
        // Start new streaming session
        const { firstChunk } = await streamingManager.startStreaming(
          this.name,
          streamingOperation,
          {
            query: finalQuery,
            groupBy,
            sortBy,
            limit,
            start_time: startTimeArg,
            end_time: endTime,
          }
        );

        return createStreamingResponse(firstChunk);
      }

      const response = await withToolTimeout(
        async () =>
          firewalla.getFlowData(finalQuery, groupBy, sortBy, limit, cursor),
        this.name
      );
      const executionTime = Date.now() - startTime;

      // Process flow data
      let processedFlows = SafeAccess.safeArrayMap(
        response.results,
        (flow: any) => ({
          timestamp: unixToISOStringOrNow(flow.ts),
          source_ip: SafeAccess.getNestedValue(
            flow,
            'source.ip',
            SafeAccess.getNestedValue(flow, 'device.ip', 'unknown')
          ),
          destination_ip: SafeAccess.getNestedValue(
            flow,
            'destination.ip',
            'unknown'
          ),
          protocol: SafeAccess.getNestedValue(flow, 'protocol', 'unknown'),
          bytes:
            (SafeAccess.getNestedValue(flow, 'download', 0) as number) +
            (SafeAccess.getNestedValue(flow, 'upload', 0) as number),
          download: SafeAccess.getNestedValue(flow, 'download', 0),
          upload: SafeAccess.getNestedValue(flow, 'upload', 0),
          packets: SafeAccess.getNestedValue(flow, 'count', 0),
          duration: SafeAccess.getNestedValue(flow, 'duration', 0),
          direction: SafeAccess.getNestedValue(flow, 'direction', 'unknown'),
          blocked: SafeAccess.getNestedValue(flow, 'block', false),
          block_type: SafeAccess.getNestedValue(flow, 'blockType', null),
          device: SafeAccess.getNestedValue(flow, 'device', {}),
          source: SafeAccess.getNestedValue(flow, 'source', {}),
          destination: SafeAccess.getNestedValue(flow, 'destination', {}),
          region: SafeAccess.getNestedValue(flow, 'region', null),
          category: SafeAccess.getNestedValue(flow, 'category', null),
        })
      );

      // Apply geographic enrichment for IP addresses
      processedFlows = await this.enrichGeoIfNeeded(processedFlows, [
        'source_ip',
        'destination_ip',
      ]);

      const actualFinalQuery = response.final_query || finalQuery;
      const hasMore = response.has_more ?? !!response.next_cursor;
      const nextCursor = hasMore ? response.next_cursor : null;
      const observationMetadata = getFlowObservationMetadata(
        response,
        actualFinalQuery,
        hasMore
      );

      // Create metadata for standardized response
      const metadata: PaginationMetadata = {
        cursor: nextCursor || undefined,
        hasMore,
        limit,
        executionTime,
        cached: false,
        source: 'firewalla_api',
        queryParams: {
          query: actualFinalQuery,
          groupBy,
          sortBy,
          limit,
          cursor,
          start_time: startTimeArg,
          end_time: endTime,
        },
        totalCount: (response as any).total_count,
      };

      // Create standardized response
      const standardResponse: any = ResponseStandardizer.toPaginatedResponse(
        processedFlows,
        metadata
      );
      standardResponse.pagination.next_cursor = nextCursor;
      standardResponse.pagination.has_more = hasMore;
      Object.assign(standardResponse.pagination, observationMetadata);
      standardResponse.pagination.pages_fetched = response.pages_fetched ?? 1;
      standardResponse.pagination.stopped_reason =
        observationMetadata.stopped_reason;
      standardResponse.pagination.repeated_cursor = response.repeated_cursor;
      standardResponse.pagination.requested_limit = response.requested_limit;
      standardResponse.pagination.applied_limit = response.applied_limit;

      return this.createUnifiedResponse(standardResponse, {
        executionTimeMs: executionTime,
      });
    } catch (error: unknown) {
      // Handle timeout errors specifically
      if (error instanceof TimeoutError) {
        return createTimeoutErrorResponse(
          this.name,
          error.duration,
          10000 // Default timeout from timeout-manager
        );
      }

      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error occurred';
      return this.createErrorResponse(
        `Failed to get flow data: ${errorMessage}`,
        ErrorType.API_ERROR,
        { originalError: errorMessage }
      );
    }
  }
}

type FlowExportStoppedReason =
  | 'no_more_results'
  | 'max_pages'
  | 'max_rows'
  | 'repeated_cursor'
  | 'api_repeated_cursor';

interface ExportableFlowRow {
  timestamp: string;
  source_ip: string;
  destination_ip: string;
  destination_name: string;
  protocol: string;
  bytes: number;
  download: number;
  upload: number;
  blocked: boolean;
  device_id: string;
  device_name: string;
  device_ip: string;
  category: string;
  region: string;
}

function toNumber(value: unknown, fallback = 0): number {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : fallback;
}

function optionalUnixTimestamp(value: unknown): number | undefined {
  const parsed = toNumber(value, Number.NaN);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined;
  }
  return parsed > 1000000000000 ? Math.floor(parsed / 1000) : parsed;
}

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  const stringValue = String(value);
  const safeValue = /^(\s*[=+\-@]|[\t\r\n])/.test(stringValue)
    ? `'${stringValue}`
    : stringValue;
  if (!/[",\n\r]/.test(safeValue)) {
    return safeValue;
  }
  return `"${safeValue.replace(/"/g, '""')}"`;
}

function safeFileComponent(value: unknown, fallback: string): string {
  const sanitized = String(value || fallback)
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  return sanitized || fallback;
}

function normalizeFlowForExport(flow: any): ExportableFlowRow {
  const download = toNumber(SafeAccess.getNestedValue(flow, 'download', 0));
  const upload = toNumber(SafeAccess.getNestedValue(flow, 'upload', 0));
  const bytes = toNumber(
    SafeAccess.getNestedValue(flow, 'bytes', download + upload)
  );

  return {
    timestamp: safeUnixToISOString(
      optionalUnixTimestamp(SafeAccess.getNestedValue(flow, 'ts', undefined)) ??
        0,
      ''
    ),
    source_ip: String(
      SafeAccess.getNestedValue(
        flow,
        'source.ip',
        SafeAccess.getNestedValue(flow, 'device.ip', 'unknown')
      )
    ),
    destination_ip: String(
      SafeAccess.getNestedValue(flow, 'destination.ip', 'unknown')
    ),
    destination_name: String(
      SafeAccess.getNestedValue(flow, 'destination.name', 'unknown')
    ),
    protocol: String(SafeAccess.getNestedValue(flow, 'protocol', 'unknown')),
    bytes,
    download,
    upload,
    blocked: Boolean(SafeAccess.getNestedValue(flow, 'block', false)),
    device_id: String(SafeAccess.getNestedValue(flow, 'device.id', 'unknown')),
    device_name: String(
      SafeAccess.getNestedValue(flow, 'device.name', 'unknown')
    ),
    device_ip: String(SafeAccess.getNestedValue(flow, 'device.ip', 'unknown')),
    category: String(SafeAccess.getNestedValue(flow, 'category', '')),
    region: String(SafeAccess.getNestedValue(flow, 'region', '')),
  };
}

function flowsToCsv(rows: ExportableFlowRow[]): string {
  const headers: Array<keyof ExportableFlowRow> = [
    'timestamp',
    'source_ip',
    'destination_ip',
    'destination_name',
    'protocol',
    'bytes',
    'download',
    'upload',
    'blocked',
    'device_id',
    'device_name',
    'device_ip',
    'category',
    'region',
  ];
  return [
    headers.join(','),
    ...rows.map(row => headers.map(header => csvEscape(row[header])).join(',')),
  ].join('\n');
}

export class ExportFlowDataHandler extends BaseToolHandler {
  name = 'export_flow_data';
  description =
    'Safely paginate Firewalla flow data to server-side raw JSON and CSV artifacts, returning only a compact export summary.';
  category = 'network' as const;

  constructor() {
    super({
      enableGeoEnrichment: false,
      enableFieldNormalization: true,
      additionalMeta: {
        data_source: 'flows',
        entity_type: 'flow_export',
        writes_artifacts: true,
        standardization_version: '2.0.0',
      },
    });
  }

  async execute(
    rawArgs: unknown,
    firewalla: FirewallaClient
  ): Promise<ToolResponse> {
    const sanitizationResult = this.sanitizeParameters(rawArgs);

    if ('errorResponse' in sanitizationResult) {
      return sanitizationResult.errorResponse;
    }

    const args = sanitizationResult.sanitizedArgs;
    const startTime = Date.now();

    try {
      const pageSizeValidation = ParameterValidator.validateNumber(
        args?.page_size ?? args?.limit,
        'page_size',
        {
          required: false,
          defaultValue: 50,
          min: 1,
          max: 50,
          integer: true,
        }
      );
      const maxPagesValidation = ParameterValidator.validateNumber(
        args?.max_pages,
        'max_pages',
        {
          required: false,
          defaultValue: 10,
          min: 1,
          max: 1000,
          integer: true,
        }
      );
      const maxRowsValidation = ParameterValidator.validateNumber(
        args?.max_rows,
        'max_rows',
        {
          required: false,
          defaultValue: 50000,
          min: 1,
          max: 1000000,
          integer: true,
        }
      );

      const validationResult = ParameterValidator.combineValidationResults([
        pageSizeValidation,
        maxPagesValidation,
        maxRowsValidation,
      ]);

      if (!validationResult.isValid) {
        return this.createErrorResponse(
          'Parameter validation failed',
          ErrorType.VALIDATION_ERROR,
          undefined,
          validationResult.errors
        );
      }

      const query = args?.query;
      const groupBy = args?.groupBy;
      const sortBy = args?.sortBy;
      const pageSize = pageSizeValidation.sanitizedValue as number;
      const maxPages = maxPagesValidation.sanitizedValue as number;
      const maxRows = maxRowsValidation.sanitizedValue as number;
      const exportBaseDir = resolve(process.cwd(), 'firewalla-flow-exports');
      const outputSubdir = args?.output_dir
        ? safeFileComponent(args.output_dir, 'export')
        : '';
      const outputDir = resolve(
        outputSubdir ? join(exportBaseDir, outputSubdir) : exportBaseDir
      );
      if (
        outputDir !== exportBaseDir &&
        !outputDir.startsWith(`${exportBaseDir}${sep}`)
      ) {
        return this.createErrorResponse(
          'Invalid output_dir',
          ErrorType.VALIDATION_ERROR,
          {
            details:
              'output_dir must resolve under the firewalla-flow-exports directory',
          },
          ['Choose a simple subdirectory name, not a parent directory path']
        );
      }
      const exportPrefix = safeFileComponent(
        args?.export_prefix,
        'flow-export'
      );
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const rawJsonPath = join(outputDir, `${exportPrefix}-${stamp}.json`);
      const csvPath = join(outputDir, `${exportPrefix}-${stamp}.csv`);

      await mkdir(outputDir, { recursive: true });

      const flows: any[] = [];
      let cursor = args?.cursor;
      let pagesFetched = 0;
      let stoppedReason: FlowExportStoppedReason = 'no_more_results';
      let repeatedCursor: string | undefined;
      let hasMore = false;
      let finalQuery: string | undefined = query;
      let totalRecordsFetched = 0;

      for (let page = 0; page < maxPages && flows.length < maxRows; page += 1) {
        const requestedCursor = cursor;
        const response = await withToolTimeout(
          async () =>
            firewalla.getFlowData(
              query,
              groupBy,
              sortBy,
              pageSize,
              requestedCursor
            ),
          this.name
        );
        const pageResults = Array.isArray(response.results)
          ? response.results
          : [];
        const remainingRows = maxRows - flows.length;
        totalRecordsFetched += pageResults.length;
        const truncatedByMaxRows = pageResults.length > remainingRows;
        flows.push(...pageResults.slice(0, remainingRows));
        pagesFetched += 1;
        finalQuery = response.final_query || finalQuery;

        if (truncatedByMaxRows) {
          stoppedReason = 'max_rows';
          hasMore = true;
          cursor = undefined;
          break;
        }

        const responseRepeatedCursor =
          response.stopped_reason === 'repeated_cursor';
        const nextCursor = response.next_cursor;
        const repeatsRequestedCursor =
          typeof requestedCursor === 'string' &&
          requestedCursor.length > 0 &&
          nextCursor === requestedCursor;

        if (responseRepeatedCursor || repeatsRequestedCursor) {
          stoppedReason = responseRepeatedCursor
            ? 'api_repeated_cursor'
            : 'repeated_cursor';
          repeatedCursor = response.repeated_cursor || requestedCursor;
          hasMore = false;
          cursor = undefined;
          break;
        }

        hasMore = response.has_more ?? !!nextCursor;
        cursor = hasMore ? nextCursor : undefined;

        if (!hasMore || !cursor) {
          stoppedReason = 'no_more_results';
          break;
        }

        if (flows.length >= maxRows && hasMore) {
          stoppedReason = 'max_rows';
          break;
        }

        if (page + 1 >= maxPages) {
          stoppedReason = 'max_pages';
          break;
        }
      }

      const rows = flows.map(normalizeFlowForExport);
      const timestamps = flows
        .map(flow =>
          optionalUnixTimestamp(
            SafeAccess.getNestedValue(flow, 'ts', undefined)
          )
        )
        .filter((ts): ts is number => typeof ts === 'number')
        .sort((a, b) => a - b);
      const totalBytes = rows.reduce((sum, row) => sum + row.bytes, 0);
      const blockedCount = rows.filter(row => row.blocked).length;
      const uniqueDestinations = new Set(
        rows
          .map(row => row.destination_ip || row.destination_name)
          .filter(Boolean)
      ).size;

      const observedStart =
        timestamps.length > 0
          ? new Date(timestamps[0] * 1000).toISOString()
          : null;
      const observedEnd =
        timestamps.length > 0
          ? new Date(timestamps[timestamps.length - 1] * 1000).toISOString()
          : null;

      const metadata = {
        exported_at: new Date().toISOString(),
        query_parameters: {
          query: finalQuery,
          groupBy,
          sortBy,
          page_size: pageSize,
          max_pages: maxPages,
          max_rows: maxRows,
          initial_cursor: args?.cursor,
        },
        row_count: flows.length,
        total_records_fetched: totalRecordsFetched,
        pages_fetched: pagesFetched,
        page_count: pagesFetched,
        stopped_reason: stoppedReason,
        has_more:
          hasMore &&
          (stoppedReason === 'max_pages' || stoppedReason === 'max_rows'),
        next_cursor: stoppedReason === 'max_pages' ? cursor : null,
        continuation_supported: stoppedReason === 'max_pages',
        truncation_note:
          stoppedReason === 'max_rows'
            ? 'Export stopped at max_rows; continuation is not exposed because the last API page may have been partially written.'
            : undefined,
        repeated_cursor: repeatedCursor,
        observed_time_range: {
          oldest: observedStart,
          newest: observedEnd,
          start: observedStart,
          end: observedEnd,
        },
        query: finalQuery,
        total_bytes: totalBytes,
        unique_destinations: uniqueDestinations,
        blocked_count: blockedCount,
      };

      await writeFile(
        rawJsonPath,
        JSON.stringify(
          {
            metadata,
            flows,
          },
          null,
          2
        ),
        'utf8'
      );
      await writeFile(csvPath, `${flowsToCsv(rows)}\n`, 'utf8');

      const summary = {
        ...metadata,
        artifacts: {
          raw_json_path: rawJsonPath,
          csv_path: csvPath,
        },
      };

      return this.createUnifiedResponse(summary, {
        executionTimeMs: Date.now() - startTime,
      });
    } catch (error: unknown) {
      if (error instanceof TimeoutError) {
        return createTimeoutErrorResponse(this.name, error.duration, 10000);
      }

      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error occurred';
      return this.createErrorResponse(
        `Failed to export flow data: ${errorMessage}`,
        ErrorType.API_ERROR,
        { originalError: errorMessage }
      );
    }
  }
}

export class GetBandwidthUsageHandler extends BaseToolHandler {
  name = 'get_bandwidth_usage';
  description =
    'Get top bandwidth consuming devices by data usage. Requires limit and period parameters. Data is cached for 5 minutes for performance.';
  category = 'network' as const;

  constructor() {
    super({
      enableGeoEnrichment: true,
      enableFieldNormalization: true,
      additionalMeta: {
        data_source: 'bandwidth_usage',
        entity_type: 'device_bandwidth',
        supports_geographic_enrichment: true,
        supports_field_normalization: true,
        standardization_version: '2.0.0',
      },
    });
  }

  async execute(
    args: ToolArgs,
    firewalla: FirewallaClient
  ): Promise<ToolResponse> {
    try {
      // Parameter validation
      const periodValidation = ParameterValidator.validateEnum(
        args?.period,
        'period',
        ['1h', '24h', '7d', '30d'],
        true
      );
      const limitValidation = ParameterValidator.validateNumber(
        args?.limit,
        'limit',
        {
          required: false,
          defaultValue: 10,
          ...getLimitValidationConfig(this.name),
        }
      );

      const validationResult = ParameterValidator.combineValidationResults([
        periodValidation,
        limitValidation,
      ]);

      if (!validationResult.isValid) {
        return this.createErrorResponse(
          'Parameter validation failed',
          ErrorType.VALIDATION_ERROR,
          undefined,
          validationResult.errors
        );
      }

      const usageResponse = await withToolTimeout(
        async () =>
          firewalla.getBandwidthUsage(
            periodValidation.sanitizedValue as string,
            limitValidation.sanitizedValue as number
          ),
        this.name
      );

      // Ensure we have results and validate count vs requested limit
      const results = usageResponse.results || [];
      const requestedLimit = limitValidation.sanitizedValue as number;

      // Note: if we get fewer results than requested, this may be due to
      // insufficient data rather than an error

      const startTime = Date.now();

      // Process bandwidth usage data
      const bandwidthData = SafeAccess.safeArrayMap(results, (item: any) => ({
        device_id: SafeAccess.getNestedValue(item, 'device_id', 'unknown'),
        device_name: SafeAccess.getNestedValue(
          item,
          'device_name',
          'Unknown Device'
        ),
        ip: SafeAccess.getNestedValue(item, 'ip', 'unknown'),
        bytes_uploaded: SafeAccess.getNestedValue(item, 'bytes_uploaded', 0),
        bytes_downloaded: SafeAccess.getNestedValue(
          item,
          'bytes_downloaded',
          0
        ),
        total_bytes: SafeAccess.getNestedValue(item, 'total_bytes', 0),
        total_mb:
          Math.round(
            ((SafeAccess.getNestedValue(item, 'total_bytes', 0) as number) /
              (1024 * 1024)) *
              100
          ) / 100,
        total_gb:
          Math.round(
            ((SafeAccess.getNestedValue(item, 'total_bytes', 0) as number) /
              (1024 * 1024 * 1024)) *
              100
          ) / 100,
      }));

      // Apply geographic enrichment for IP addresses
      const enrichedBandwidthData = await this.enrichGeoIfNeeded(
        bandwidthData,
        ['ip']
      );

      const unifiedResponseData = {
        period: periodValidation.sanitizedValue,
        top_devices: results.length,
        requested_limit: requestedLimit,
        bandwidth_usage: enrichedBandwidthData,
      };

      const executionTime = Date.now() - startTime;
      return this.createUnifiedResponse(unifiedResponseData, {
        executionTimeMs: executionTime,
      });
    } catch (error: unknown) {
      // Handle timeout errors specifically
      if (error instanceof TimeoutError) {
        return createTimeoutErrorResponse(
          this.name,
          error.duration,
          10000 // Default timeout from timeout-manager
        );
      }

      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error occurred';
      return this.createErrorResponse(
        `Failed to get bandwidth usage: ${errorMessage}`,
        ErrorType.API_ERROR,
        { originalError: errorMessage }
      );
    }
  }
}

export class GetOfflineDevicesHandler extends BaseToolHandler {
  name = 'get_offline_devices';
  description =
    'Get all offline devices with last seen timestamps and detailed device information. Requires limit parameter. Data cached for 2 minutes for performance.';
  category = 'network' as const;

  constructor() {
    super({
      enableGeoEnrichment: true,
      enableFieldNormalization: true,
      additionalMeta: {
        data_source: 'devices',
        entity_type: 'offline_devices',
        supports_geographic_enrichment: true,
        supports_field_normalization: true,
        standardization_version: '2.0.0',
      },
    });
  }

  async execute(
    args: ToolArgs,
    firewalla: FirewallaClient
  ): Promise<ToolResponse> {
    try {
      // Parameter validation with standardized limits
      const limitValidation = ParameterValidator.validateNumber(
        args?.limit,
        'limit',
        {
          required: false,
          defaultValue: 100,
          ...getLimitValidationConfig(this.name),
        }
      );
      const sortValidation = ParameterValidator.validateBoolean(
        args?.sort_by_last_seen,
        'sort_by_last_seen',
        true
      );

      const validationResult = ParameterValidator.combineValidationResults([
        limitValidation,
        sortValidation,
      ]);

      if (!validationResult.isValid) {
        return this.createErrorResponse(
          'Parameter validation failed',
          ErrorType.VALIDATION_ERROR,
          undefined,
          validationResult.errors
        );
      }

      const limit = limitValidation.sanitizedValue! as number;
      const sortByLastSeen = sortValidation.sanitizedValue ?? true;

      // Buffer Strategy: Fetch extra devices to account for post-processing filtering
      //
      // Problem: When filtering for offline devices, we don't know how many devices
      // are offline until after fetching. If we only fetch the requested limit,
      // we might get fewer results than requested after filtering.
      //
      // Solution: Use a "buffer multiplier" strategy where we fetch 3x the requested
      // limit to increase the probability of having enough offline devices after
      // filtering. This trades some API overhead for more consistent result counts.
      //
      // The multiplier of 3 is empirically chosen based on typical online/offline
      // ratios in network environments (usually 60-80% devices are online).
      const fetchLimit = Math.min(limit * 3, 1000); // 3x buffer with 1000 cap for API limits
      const allDevicesResponse = await withToolTimeout(
        async () => firewalla.getDeviceStatus(undefined, undefined, fetchLimit),
        this.name
      );

      // Normalize device data for consistency first
      const deviceResults = SafeAccess.safeArrayAccess(
        allDevicesResponse.results,
        (arr: any[]) => arr,
        []
      ) as any[];

      const normalizedDevices = batchNormalize(deviceResults, {
        name: (v: any) => sanitizeFieldValue(v, 'Unknown Device').value,
        ip: (v: any) => sanitizeFieldValue(v, 'unknown').value,
        macVendor: (v: any) => sanitizeFieldValue(v, 'unknown').value,
        network: (v: any) => (v ? normalizeUnknownFields(v) : null),
        group: (v: any) => (v ? normalizeUnknownFields(v) : null),
        online: (v: any) => Boolean(v), // Ensure consistent boolean handling
      });

      // Filter to only offline devices with consistent boolean checking
      let offlineDevices = SafeAccess.safeArrayFilter(
        normalizedDevices,
        (device: any) => device.online === false
      );

      // Sort by last seen timestamp if requested
      if (sortByLastSeen) {
        offlineDevices = offlineDevices.sort((a, b) => {
          const aTime = Number(SafeAccess.getNestedValue(a, 'lastSeen', 0));
          const bTime = Number(SafeAccess.getNestedValue(b, 'lastSeen', 0));
          return bTime - aTime; // Most recent first
        });
      }

      // Apply the requested limit
      const limitedOfflineDevices = offlineDevices.slice(0, limit);

      const responseStartTime = Date.now();

      // Process device data
      const deviceData = SafeAccess.safeArrayMap(
        limitedOfflineDevices,
        (device: any) => ({
          id: SafeAccess.getNestedValue(device, 'id', 'unknown'),
          gid: SafeAccess.getNestedValue(device, 'gid', 'unknown'),
          name: device.name, // Already normalized
          ip: device.ip, // Already normalized
          macVendor: device.macVendor, // Already normalized
          online: device.online, // Already normalized to false for offline devices
          lastSeen: SafeAccess.getNestedValue(device, 'lastSeen', 0),
          lastSeenFormatted: safeUnixToISOString(
            SafeAccess.getNestedValue(device, 'lastSeen', 0) as number,
            'Never'
          ),
          ipReserved: SafeAccess.getNestedValue(device, 'ipReserved', false),
          network: device.network, // Already normalized
          group: device.group, // Already normalized
          totalDownload: sanitizeByteCount(
            SafeAccess.getNestedValue(device, 'totalDownload', 0)
          ),
          totalUpload: sanitizeByteCount(
            SafeAccess.getNestedValue(device, 'totalUpload', 0)
          ),
        })
      );

      // Apply geographic enrichment for IP addresses
      const enrichedDeviceData = await this.enrichGeoIfNeeded(deviceData, [
        'ip',
      ]);

      const unifiedResponseData = {
        total_offline_devices: offlineDevices.length,
        limit_applied: limit,
        returned_count: limitedOfflineDevices.length,
        devices: enrichedDeviceData,
      };

      const executionTime = Date.now() - responseStartTime;
      return this.createUnifiedResponse(unifiedResponseData, {
        executionTimeMs: executionTime,
      });
    } catch (error: unknown) {
      // Handle timeout errors specifically
      if (error instanceof TimeoutError) {
        return createTimeoutErrorResponse(
          this.name,
          error.duration,
          10000 // Default timeout from timeout-manager
        );
      }

      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error occurred';
      return this.createErrorResponse(
        `Failed to get offline devices: ${errorMessage}`,
        ErrorType.API_ERROR,
        { originalError: errorMessage }
      );
    }
  }
}
