/**
 * Controller for executing Flink SQL cells
 */

import * as vscode from 'vscode';
import { SqlGatewayClient } from '../services/sqlGatewayClient';

export interface StatementExecutedEvent {
  sql: string;
  success: boolean;
  error?: Error;
}

interface StreamingCellInfo {
  cell: vscode.NotebookCell;
  execution: vscode.NotebookCellExecution;
  sessionId: string;
  statementId: string;
  jobId?: string;
  stopRequested: boolean;
  paused: boolean;
}

export class FlinkNotebookController {
  private controller: vscode.NotebookController;
  private executionOrder = 0;
  private _onStatementExecuted = new vscode.EventEmitter<StatementExecutedEvent>();
  private streamingCells: Map<string, StreamingCellInfo> = new Map();

  /**
   * Event fired after a statement is executed (successfully or with error)
   */
  readonly onStatementExecuted = this._onStatementExecuted.event;

  constructor(
    private gatewayClient: SqlGatewayClient,
    private sessionManager: SessionManager
  ) {
    this.controller = vscode.notebooks.createNotebookController(
      'flink-notebook-controller',
      'flink-notebook',
      'Flink SQL'
    );

    this.controller.supportedLanguages = ['flink-sql', 'sql'];
    this.controller.supportsExecutionOrder = true;
    this.controller.description = 'Execute Flink SQL queries';
    this.controller.executeHandler = this.execute.bind(this);
  }

  private async execute(
    cells: vscode.NotebookCell[],
    _notebook: vscode.NotebookDocument,
    _controller: vscode.NotebookController
  ): Promise<void> {
    for (const cell of cells) {
      await this.executeCell(cell);
    }
  }

  private async executeCell(cell: vscode.NotebookCell): Promise<void> {
    const execution = this.controller.createNotebookCellExecution(cell);
    execution.executionOrder = ++this.executionOrder;
    execution.start(Date.now());

    const sql = cell.document.getText().trim();

    try {
      if (!sql) {
        execution.end(true, Date.now());
        return;
      }

      // Get or create session
      const sessionId = await this.sessionManager.getOrCreateSession();

      // Execute SQL
      const response = await this.gatewayClient.executeStatement(sessionId, sql);
      const statementId = response.operationHandle;

      // Store statement ID in cell metadata
      await this.updateCellMetadata(cell, { statement_id: statementId });

      // Clear previous outputs
      execution.clearOutput(cell);

      // Fetch and display results
      await this.fetchResults(cell, execution, sessionId, statementId);

      execution.end(true, Date.now());

      // Fire event for successful execution
      this._onStatementExecuted.fire({
        sql,
        success: true,
      });
    } catch (error) {
      let errorMessage = error instanceof Error ? error.message : String(error);

      // If it's an axios error, extract more details
      if (error && typeof error === 'object' && 'response' in error) {
        const axiosError = error as any;
        if (axiosError.response?.data) {
          console.error('Gateway error response:', axiosError.response.data);
          errorMessage += '\n\nGateway error: ' + JSON.stringify(axiosError.response.data, null, 2);
        }
      }

      execution.replaceOutput(
        new vscode.NotebookCellOutput([
          vscode.NotebookCellOutputItem.error(new Error(errorMessage)),
        ])
      );
      execution.end(false, Date.now());

      // Fire event for failed execution
      this._onStatementExecuted.fire({
        sql,
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
      });
    }
  }

  private async fetchResults(
    cell: vscode.NotebookCell,
    execution: vscode.NotebookCellExecution,
    sessionId: string,
    statementId: string
  ): Promise<void> {
    const rows: unknown[] = [];
    let columns: any[] | undefined;
    let token = 0;

    try {
      // Wait for operation to be ready
      let statusInfo;
      let attempts = 0;
      const maxAttempts = 60; // 60 seconds timeout

      while (attempts < maxAttempts) {
        statusInfo = await this.gatewayClient.getStatementInfo(sessionId, statementId);
        console.log(`Operation status: ${statusInfo.status}`);

        if (statusInfo.status === 'ERROR') {
          throw new Error('Operation failed');
        }

        if (statusInfo.status === 'FINISHED' || statusInfo.status === 'RUNNING') {
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));
        attempts++;
      }

      // Give Flink's result store time to materialize results after operation completes
      // This is especially important for external catalogs (Iceberg, Hive, etc.)
      console.log('Operation ready, waiting for result materialization...');
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Fetch first batch - wait for results to be ready
      // For external catalogs (Iceberg, etc), there can be significant delay
      // between operation completion and result materialization
      let fetchAttempts = 0;
      const maxFetchAttempts = 60; // Wait up to 30 seconds for results to be ready
      const fetchRetryDelay = 500; // 500ms between retries
      let firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);

      while (fetchAttempts < maxFetchAttempts) {
        console.log(`Fetch attempt ${fetchAttempts + 1}/${maxFetchAttempts} (token ${token}):`);
        console.log(`  resultType: ${firstResult.resultType}`);
        console.log(`  isQueryResult: ${firstResult.isQueryResult}`);
        console.log(`  nextResultUri: ${firstResult.nextResultUri}`);
        console.log(`  results.data length: ${firstResult.results?.data?.length || 0}`);

        const rawData = firstResult.results?.data || firstResult.data || [];

        // Per OpenAPI spec, resultType can be: NOT_READY, PAYLOAD, or EOS

        // Case 1: EOS - End of Stream, no more data
        if (firstResult.resultType === 'EOS') {
          console.log('Received EOS (End of Stream) - query complete');
          break;
        }

        // Case 2: PAYLOAD with data - success!
        if (firstResult.resultType === 'PAYLOAD' && rawData.length > 0) {
          console.log(`Received PAYLOAD with ${rawData.length} rows`);
          break;
        }

        // Case 3: PAYLOAD with no data - this token is empty, try next token if available
        if (firstResult.resultType === 'PAYLOAD' && rawData.length === 0) {
          if (firstResult.nextResultUri) {
            // Parse next token from URI
            const nextUriToken = parseInt(firstResult.nextResultUri.split('/').pop() || '0');
            if (nextUriToken > token) {
              console.log(`Token ${token} empty, advancing to token ${nextUriToken}`);
              token = nextUriToken;
              fetchAttempts++;
              firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
              continue;
            } else {
              // nextResultUri points to same or earlier token - this means we're waiting for data
              console.log(`Empty PAYLOAD at token ${token}, nextResultUri=${firstResult.nextResultUri}, waiting...`);
              await new Promise((resolve) => setTimeout(resolve, fetchRetryDelay));
              fetchAttempts++;
              firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
              continue;
            }
          } else {
            // No nextResultUri and no data - wait for data to materialize
            console.log(`Empty PAYLOAD at token ${token}, no nextResultUri, waiting for data...`);
            await new Promise((resolve) => setTimeout(resolve, fetchRetryDelay));
            fetchAttempts++;
            firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
            continue;
          }
        }

        // Case 4: NOT_READY - results not materialized yet, wait and retry same token
        if (firstResult.resultType === 'NOT_READY') {
          console.log(`NOT_READY at token ${token}, waiting for materialization...`);
          await new Promise((resolve) => setTimeout(resolve, fetchRetryDelay));
          fetchAttempts++;
          firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
          continue;
        }

        // Unexpected state - log and retry
        console.log(`Unexpected state: resultType=${firstResult.resultType}, retrying...`);
        await new Promise((resolve) => setTimeout(resolve, fetchRetryDelay));
        fetchAttempts++;
        firstResult = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
      }

      if (fetchAttempts >= maxFetchAttempts) {
        console.log(`⚠️  Timeout waiting for results to be ready after ${maxFetchAttempts} attempts (${maxFetchAttempts * fetchRetryDelay / 1000}s)`);
        console.log(`This may indicate results are not materializing. Proceeding with empty result set.`);
      }

      // Extract columns
      if (firstResult.results?.columns) {
        columns = firstResult.results.columns;
      }

      // Transform first batch
      const rawData = firstResult.results?.data || firstResult.data || [];
      if (rawData.length > 0 && columns) {
        const transformedRows = this.transformRows(rawData, columns);
        rows.push(...transformedRows);
        console.log(`Fetched ${transformedRows.length} rows in first batch`);
      }

      // Detect if streaming: has more results but not finished
      const isStreaming =
        firstResult.resultType !== 'EOS' &&
        firstResult.nextResultUri !== undefined &&
        statusInfo?.status === 'RUNNING';

      if (isStreaming) {
        console.log('Detected streaming query, enabling streaming mode');

        // Update metadata to mark as streaming
        await this.updateCellMetadata(cell, {
          is_streaming: true,
          streaming_started: Date.now(),
          total_rows_fetched: rows.length,
        });

        // Track this cell for streaming
        const cellKey = this.getCellKey(cell);
        const jobId = firstResult.jobID;
        this.streamingCells.set(cellKey, {
          cell,
          execution,
          sessionId,
          statementId,
          jobId,
          stopRequested: false,
          paused: false,
        });

        // Set context for button visibility (initially not paused)
        await vscode.commands.executeCommand('setContext', 'flink-notebooks:streamingPaused', false);

        // Display first batch immediately
        this.updateStreamingOutput(cell, execution, rows, columns);

        // Extract next token
        let nextToken = firstResult.nextResultUri;
        if (nextToken && nextToken.includes('/')) {
          token = parseInt(nextToken.split('/').pop() || '0');
        } else {
          token = 1;
        }

        // Fetch streaming results
        await this.fetchStreamingResults(
          cell,
          execution,
          sessionId,
          statementId,
          rows,
          columns,
          token
        );
      } else {
        // Batch query - fetch remaining results
        console.log('Detected batch query, fetching all results');

        let nextToken = firstResult.nextResultUri;
        let isComplete = firstResult.resultType === 'EOS' || !nextToken;

        if (!isComplete) {
          // More results to fetch
          if (nextToken && nextToken.includes('/')) {
            token = parseInt(nextToken.split('/').pop() || '0');
          } else {
            token = 1;
          }

          await this.fetchBatchResults(
            cell,
            execution,
            sessionId,
            statementId,
            rows,
            columns,
            token
          );
        } else {
          // All results fetched in first batch
          console.log('All batch results fetched in first batch');
          this.updateCellOutput(cell, execution, rows, columns);
        }
      }
    } catch (error) {
      throw error;
    }
  }

  /**
   * Transform raw Flink rows to objects
   */
  private transformRows(rawData: any[], columns: any[]): unknown[] {
    return rawData.map((row: any) => {
      const fields = row.fields || [];
      const obj: Record<string, any> = {};

      columns.forEach((col: any, index: number) => {
        obj[col.name] = fields[index];
      });

      return obj;
    });
  }

  /**
   * Fetch all results for a batch query
   */
  private async fetchBatchResults(
    cell: vscode.NotebookCell,
    execution: vscode.NotebookCellExecution,
    sessionId: string,
    statementId: string,
    rows: unknown[],
    columns: any[] | undefined,
    startToken: number
  ): Promise<void> {
    let token = startToken;
    let isComplete = false;

    while (!isComplete) {
      console.log(`Fetching batch results: token=${token}`);
      const result = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);

      const rawData = result.results?.data || result.data || [];
      if (rawData.length > 0 && columns) {
        const transformedRows = this.transformRows(rawData, columns);
        rows.push(...transformedRows);

        // Update output periodically
        if (rows.length % 10 === 0) {
          this.updateCellOutput(cell, execution, rows, columns);
        }
      }

      // Check if more results available
      const nextToken = result.nextResultUri;
      isComplete = result.resultType === 'EOS' || !nextToken;

      if (!isComplete) {
        if (nextToken && nextToken.includes('/')) {
          token = parseInt(nextToken.split('/').pop() || '0');
        } else {
          token += 1;
        }

        // Small delay before next fetch
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Final output update
    this.updateCellOutput(cell, execution, rows, columns);
  }

  /**
   * Fetch streaming results with continuous polling
   */
  private async fetchStreamingResults(
    cell: vscode.NotebookCell,
    execution: vscode.NotebookCellExecution,
    sessionId: string,
    statementId: string,
    rows: unknown[],
    columns: any[] | undefined,
    startToken: number
  ): Promise<void> {
    let token = startToken;
    let isComplete = false;
    let wasCancelled = false;
    const cellKey = this.getCellKey(cell);
    const config = vscode.workspace.getConfiguration('flink-notebooks');
    const pollInterval = config.get<number>('streamingPollInterval', 500);
    const maxRows = config.get<number>('maxStreamingRows', 10000);

    console.log(`Starting streaming results for cell ${cellKey}`);

    while (!isComplete) {
      // Check if stop was requested
      const streamingInfo = this.streamingCells.get(cellKey);
      if (streamingInfo?.stopRequested) {
        console.log('Streaming stopped by user');

        // Display canceled status
        const outputs: vscode.NotebookCellOutputItem[] = [];
        const header = `🛑 **CANCELED** | ${rows.length.toLocaleString()} rows fetched\n\n`;
        const markdown = header + this.createMarkdownTable(rows, columns);
        outputs.push(vscode.NotebookCellOutputItem.text(markdown, 'text/markdown'));
        outputs.push(
          vscode.NotebookCellOutputItem.json(
            { rows, columns, status: 'CANCELED' },
            'application/vnd.flink-table+json'
          )
        );
        execution.replaceOutput(new vscode.NotebookCellOutput(outputs), cell);

        wasCancelled = true;
        break;
      }

      // Check if streaming is paused
      if (streamingInfo?.paused) {
        // Wait a bit and check again (don't poll for new data while paused)
        await new Promise((resolve) => setTimeout(resolve, pollInterval));
        continue;
      }

      try {
        const result = await this.gatewayClient.fetchResults(sessionId, statementId, token, 100);
        const rawData = result.results?.data || result.data || [];

        // Transform and append new rows
        if (rawData.length > 0 && columns) {
          const transformedRows = this.transformRows(rawData, columns);
          rows.push(...transformedRows);

          // Update streaming output with new rows
          this.updateStreamingOutput(cell, execution, rows, columns);

          // Update metadata with row count
          await this.updateCellMetadata(cell, {
            total_rows_fetched: rows.length,
          });
        }

        // Check if complete
        const nextToken = result.nextResultUri;
        isComplete = result.resultType === 'EOS' || !nextToken;

        if (!isComplete) {
          // Extract next token
          if (nextToken && nextToken.includes('/')) {
            token = parseInt(nextToken.split('/').pop() || '0');
          } else {
            token += 1;
          }

          // Check row limit
          if (maxRows > 0 && rows.length >= maxRows) {
            console.log(`Reached max streaming rows (${maxRows})`);
            const choice = await vscode.window.showWarningMessage(
              `Reached maximum ${maxRows.toLocaleString()} rows. Continue streaming?`,
              'Continue',
              'Stop'
            );

            if (choice !== 'Continue') {
              break;
            }
          }

          // Streaming delay
          await new Promise((resolve) => setTimeout(resolve, pollInterval));
        }
      } catch (error) {
        // Check if the operation was cancelled or failed before logging the error
        let isCancellationError = false;

        try {
          const statusInfo = await this.gatewayClient.getStatementInfo(sessionId, statementId);
          console.log(`Operation status after error: ${statusInfo.status}`);

          if (statusInfo.status === 'ERROR' || statusInfo.status === 'CANCELED') {
            isCancellationError = statusInfo.status === 'CANCELED';
            console.log(`Streaming stopped: operation ${statusInfo.status}`);

            // Display canceled/error status
            const statusMessage = statusInfo.status === 'CANCELED'
              ? '🛑 **CANCELED**'
              : '❌ **ERROR**';
            const outputs: vscode.NotebookCellOutputItem[] = [];
            const header = `${statusMessage} | ${rows.length.toLocaleString()} rows fetched\n\n`;
            const markdown = header + this.createMarkdownTable(rows, columns);
            outputs.push(vscode.NotebookCellOutputItem.text(markdown, 'text/markdown'));
            outputs.push(
              vscode.NotebookCellOutputItem.json(
                { rows, columns, status: statusInfo.status },
                'application/vnd.flink-table+json'
              )
            );
            execution.replaceOutput(new vscode.NotebookCellOutput(outputs), cell);

            // Mark as cancelled and stop streaming
            wasCancelled = true;
            break;
          }
        } catch (statusError) {
          // If we can't check status, the operation might have been deleted (cancelled)
          // Stop streaming to avoid infinite error loop
          isCancellationError = true;
          console.log('Streaming stopped: operation no longer exists');

          const outputs: vscode.NotebookCellOutputItem[] = [];
          const header = `🛑 **CANCELED** | ${rows.length.toLocaleString()} rows fetched\n\n`;
          const markdown = header + this.createMarkdownTable(rows, columns);
          outputs.push(vscode.NotebookCellOutputItem.text(markdown, 'text/markdown'));
          outputs.push(
            vscode.NotebookCellOutputItem.json(
              { rows, columns, status: 'CANCELED' },
              'application/vnd.flink-table+json'
            )
          );
          execution.replaceOutput(new vscode.NotebookCellOutput(outputs), cell);

          // Mark as cancelled and stop streaming
          wasCancelled = true;
          break;
        }

        // Log unexpected errors (not cancellations)
        if (!isCancellationError) {
          console.error('Error fetching streaming results (will retry):', error);
          // For transient errors, retry after delay
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }
    }

    // Final update with regular output (remove streaming indicator) - only if not cancelled
    if (!wasCancelled) {
      this.updateCellOutput(cell, execution, rows, columns);
    }

    // Clean up streaming tracking
    this.streamingCells.delete(cellKey);

    console.log(`Streaming completed for cell ${cellKey}: ${rows.length} total rows`);
  }

  /**
   * Update output with streaming indicator
   */
  private updateStreamingOutput(
    cell: vscode.NotebookCell,
    execution: vscode.NotebookCellExecution,
    rows: unknown[],
    columns?: any[]
  ): void {
    const startTime = cell.metadata?.streaming_started || Date.now();
    const duration = this.formatDuration(Date.now() - startTime);
    const isPaused = cell.metadata?.streaming_paused === true;

    const outputs: vscode.NotebookCellOutputItem[] = [];

    // Streaming header with pause indicator
    const statusIcon = isPaused ? '⏸️' : '🟢';
    const statusText = isPaused ? '**Paused**' : '**Streaming**';
    const header = `${statusIcon} ${statusText} | ${rows.length.toLocaleString()} rows | ${duration}\n\n`;

    // Create markdown table with header
    const markdown = header + this.createMarkdownTable(rows, columns);
    outputs.push(vscode.NotebookCellOutputItem.text(markdown, 'text/markdown'));

    // Add JSON data
    outputs.push(
      vscode.NotebookCellOutputItem.json(
        { rows, columns, streaming: true, paused: isPaused },
        'application/vnd.flink-table+json'
      )
    );

    execution.replaceOutput(new vscode.NotebookCellOutput(outputs), cell);
  }

  private updateCellOutput(
    cell: vscode.NotebookCell,
    execution: vscode.NotebookCellExecution,
    rows: unknown[],
    columns?: any[]
  ): void {
    // Create output items
    const outputs: vscode.NotebookCellOutputItem[] = [];

    if (rows.length === 0) {
      outputs.push(vscode.NotebookCellOutputItem.text('0 rows', 'text/plain'));
    } else {
      // Create markdown table
      const markdown = this.createMarkdownTable(rows, columns);
      outputs.push(vscode.NotebookCellOutputItem.text(markdown, 'text/markdown'));

      // Add table data as JSON (for programmatic access)
      outputs.push(
        vscode.NotebookCellOutputItem.json(
          { rows, columns },
          'application/vnd.flink-table+json'
        )
      );
    }

    execution.replaceOutput(new vscode.NotebookCellOutput(outputs), cell);
  }

  private createMarkdownTable(rows: unknown[], columns?: any[]): string {
    if (rows.length === 0) {
      return '0 rows';
    }

    const lines: string[] = [];

    // Get column names
    const columnNames: string[] = [];
    if (columns && columns.length > 0) {
      // Use column info from SQL Gateway
      columnNames.push(...columns.map((col: any) => col.name));
    } else if (rows.length > 0) {
      // Infer from first row
      const firstRow = rows[0] as Record<string, any>;
      if (firstRow && typeof firstRow === 'object') {
        columnNames.push(...Object.keys(firstRow));
      }
    }

    if (columnNames.length === 0) {
      return `${rows.length} row${rows.length !== 1 ? 's' : ''}`;
    }

    // Header row
    lines.push('| ' + columnNames.join(' | ') + ' |');

    // Separator row
    lines.push('| ' + columnNames.map(() => '---').join(' | ') + ' |');

    // Data rows
    for (const row of rows) {
      const values = columnNames.map((colName) => {
        const value = (row as Record<string, any>)[colName];
        if (value === null || value === undefined) {
          return 'NULL';
        }
        return String(value);
      });
      lines.push('| ' + values.join(' | ') + ' |');
    }

    // Add row count footer
    lines.push('');
    lines.push(`*${rows.length} row${rows.length !== 1 ? 's' : ''}*`);

    return lines.join('\n');
  }

  private async updateCellMetadata(
    cell: vscode.NotebookCell,
    metadata: Record<string, unknown>
  ): Promise<void> {
    const edit = new vscode.WorkspaceEdit();
    const notebookEdit = vscode.NotebookEdit.updateCellMetadata(cell.index, {
      ...cell.metadata,
      ...metadata,
    });
    edit.set(cell.notebook.uri, [notebookEdit]);
    await vscode.workspace.applyEdit(edit);
  }

  async interruptCell(cell: vscode.NotebookCell): Promise<void> {
    const cellKey = this.getCellKey(cell);
    const streamingInfo = this.streamingCells.get(cellKey);

    if (streamingInfo) {
      // Mark as stop requested
      streamingInfo.stopRequested = true;

      // Cancel the operation
      try {
        await this.gatewayClient.cancelOperation(
          streamingInfo.sessionId,
          streamingInfo.statementId
        );
      } catch (error) {
        console.error('Error canceling streaming operation:', error);
      }

      // Clean up
      this.streamingCells.delete(cellKey);
    }

    // Also try canceling via metadata (fallback)
    const statementId = cell.metadata?.statement_id as string | undefined;
    if (statementId) {
      const sessionId = await this.sessionManager.getCurrentSessionId();
      if (sessionId) {
        try {
          await this.gatewayClient.cancelOperation(sessionId, statementId);
        } catch (error) {
          console.error('Error canceling operation:', error);
        }
      }
    }
  }

  /**
   * Stop streaming for a specific cell
   */
  async stopStreaming(cell: vscode.NotebookCell): Promise<void> {
    const cellKey = this.getCellKey(cell);
    const streamingInfo = this.streamingCells.get(cellKey);

    if (streamingInfo) {
      streamingInfo.stopRequested = true;
      console.log(`Streaming stop requested for cell ${cellKey}`);
    }
  }

  /**
   * Pause streaming for a specific cell (keeps job running, stops polling)
   */
  async pauseStreaming(cell: vscode.NotebookCell): Promise<void> {
    const cellKey = this.getCellKey(cell);
    const streamingInfo = this.streamingCells.get(cellKey);

    if (streamingInfo) {
      streamingInfo.paused = true;
      await this.updateCellMetadata(cell, {
        streaming_paused: true,
      });

      // Set context for button visibility
      await vscode.commands.executeCommand('setContext', 'flink-notebooks:streamingPaused', true);

      // Refresh UI to show paused state
      const rows = cell.metadata?.total_rows_fetched || 0;
      const rawRows = cell.outputs?.[0]?.items?.[1]?.data;
      if (rawRows) {
        try {
          const data = JSON.parse(new TextDecoder().decode(rawRows as Uint8Array));
          this.updateStreamingOutput(cell, streamingInfo.execution, data.rows, data.columns);
        } catch (error) {
          console.error('Error refreshing paused output:', error);
        }
      }

      console.log(`Streaming paused for cell ${cellKey}`);
    }
  }

  /**
   * Resume streaming for a specific cell
   */
  async resumeStreaming(cell: vscode.NotebookCell): Promise<void> {
    const cellKey = this.getCellKey(cell);
    const streamingInfo = this.streamingCells.get(cellKey);

    if (streamingInfo) {
      streamingInfo.paused = false;
      await this.updateCellMetadata(cell, {
        streaming_paused: false,
      });

      // Set context for button visibility
      await vscode.commands.executeCommand('setContext', 'flink-notebooks:streamingPaused', false);

      // Refresh UI to show streaming state
      const rawRows = cell.outputs?.[0]?.items?.[1]?.data;
      if (rawRows) {
        try {
          const data = JSON.parse(new TextDecoder().decode(rawRows as Uint8Array));
          this.updateStreamingOutput(cell, streamingInfo.execution, data.rows, data.columns);
        } catch (error) {
          console.error('Error refreshing resumed output:', error);
        }
      }

      console.log(`Streaming resumed for cell ${cellKey}`);
    }
  }

  /**
   * Cancel the operation/job for a specific cell
   */
  async cancelOperation(cell: vscode.NotebookCell): Promise<void> {
    const cellKey = this.getCellKey(cell);
    const streamingInfo = this.streamingCells.get(cellKey);

    if (streamingInfo) {
      // Mark as stop requested to exit polling loop
      streamingInfo.stopRequested = true;

      // Cancel the operation via SQL Gateway
      try {
        await this.gatewayClient.cancelOperation(
          streamingInfo.sessionId,
          streamingInfo.statementId
        );
        console.log(`Operation canceled for cell ${cellKey}`);
      } catch (error) {
        console.error('Error canceling operation:', error);
        throw error;
      }

      // Clear context for button visibility
      await vscode.commands.executeCommand('setContext', 'flink-notebooks:streamingPaused', false);

      // Clean up
      this.streamingCells.delete(cellKey);
    }
  }

  /**
   * Stop streaming for all cells associated with a job ID
   */
  async stopStreamingByJobId(jobId: string): Promise<void> {
    let stoppedCount = 0;

    for (const [cellKey, streamingInfo] of this.streamingCells.entries()) {
      if (streamingInfo.jobId === jobId) {
        console.log(`Stopping streaming for cell ${cellKey} (job ${jobId})`);
        streamingInfo.stopRequested = true;
        stoppedCount++;
      }
    }

    if (stoppedCount > 0) {
      console.log(`Stopped streaming for ${stoppedCount} cell(s) associated with job ${jobId}`);
    }
  }

  /**
   * Generate unique key for a cell
   */
  private getCellKey(cell: vscode.NotebookCell): string {
    return `${cell.notebook.uri.toString()}-${cell.index}`;
  }

  /**
   * Format duration in human-readable format
   */
  private formatDuration(durationMs: number): string {
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  dispose(): void {
    // Stop all streaming cells
    for (const [key, info] of this.streamingCells.entries()) {
      info.stopRequested = true;
    }
    this.streamingCells.clear();

    this._onStatementExecuted.dispose();
    this.controller.dispose();
  }
}

/**
 * Manages SQL sessions for notebooks
 */
export class SessionManager {
  private currentSessionId: string | null = null;

  constructor(private gatewayClient: SqlGatewayClient) {}

  async getOrCreateSession(): Promise<string> {
    if (this.currentSessionId) {
      try {
        // Verify session still exists
        await this.gatewayClient.getSession(this.currentSessionId);
        return this.currentSessionId;
      } catch {
        // Session no longer valid, create new one
        this.currentSessionId = null;
      }
    }

    // Get execution mode from configuration
    const config = vscode.workspace.getConfiguration('flink-notebooks');
    const executionMode = config.get<string>('executionMode', 'auto');

    // Create session with optional execution mode
    const properties: Record<string, string> = {};
    if (executionMode !== 'auto') {
      properties['execution.runtime-mode'] = executionMode;
      console.log(`Creating Flink session with execution mode: ${executionMode}`);
    } else {
      console.log('Creating Flink session with auto execution mode (Flink decides)');
    }

    const session = await this.gatewayClient.createSession('notebook-session', properties);
    this.currentSessionId = session.sessionHandle;
    return this.currentSessionId;
  }

  getCurrentSessionId(): string | null {
    return this.currentSessionId;
  }

  async closeSession(): Promise<void> {
    if (this.currentSessionId) {
      await this.gatewayClient.closeSession(this.currentSessionId);
      this.currentSessionId = null;
    }
  }
}
