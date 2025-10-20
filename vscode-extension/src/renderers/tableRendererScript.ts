/**
 * Flink Table Renderer - Webview Script
 *
 * This script runs in the notebook cell output webview and renders
 * interactive HTML tables with sorting, filtering, and pagination.
 */

// VSCode Notebook Renderer API types
interface RendererContext {
  readonly setState: (state: any) => void;
  readonly getState: () => any | undefined;
  readonly getRenderer: (mimeType: string) => any;
  readonly workspace: {
    readonly isTrusted: boolean;
  };
  readonly settings: {
    readonly lineLimit: number;
  };
}

interface ActivationFunction {
  (ctx: RendererContext): NotebookOutputRenderer;
}

interface NotebookOutputRenderer {
  renderOutputItem(outputItem: OutputItem, element: HTMLElement): void;
  disposeOutputItem?(outputId?: string): void;
}

interface OutputItem {
  id: string;
  mime: string;
  text(): string;
  json(): any;
  blob(): Blob;
}

// Column metadata from Flink SQL Gateway
interface ColumnInfo {
  name: string;
  logicalType: {
    type: string;
    nullable: boolean;
  };
}

// Table data structure
interface FlinkTableData {
  rows: any[];
  columns?: ColumnInfo[];
  streaming?: boolean;
  status?: string;
}

// Renderer state
interface TableState {
  currentPage: number;
  rowsPerPage: number;
}

// Get Flink type display name
function getTypeDisplayName(type: string): string {
  const typeMap: Record<string, string> = {
    'VARCHAR': 'STRING',
    'CHAR': 'STRING',
    'INTEGER': 'INT',
    'BIGINT': 'LONG',
    'DOUBLE': 'DOUBLE',
    'FLOAT': 'FLOAT',
    'DECIMAL': 'DECIMAL',
    'BOOLEAN': 'BOOL',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP_LTZ': 'TIMESTAMP',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'BINARY': 'BINARY',
    'VARBINARY': 'BINARY',
  };

  return typeMap[type.toUpperCase()] || type.toUpperCase();
}

// Get color for type badge
function getTypeColor(type: string): string {
  const upperType = type.toUpperCase();

  if (upperType.includes('INT') || upperType.includes('LONG')) {
    return '#3794ff'; // Blue
  }
  if (upperType.includes('DECIMAL') || upperType.includes('DOUBLE') || upperType.includes('FLOAT')) {
    return '#89d185'; // Green
  }
  if (upperType.includes('TIMESTAMP') || upperType.includes('DATE') || upperType.includes('TIME')) {
    return '#b180d7'; // Purple
  }
  if (upperType.includes('STRING') || upperType.includes('CHAR') || upperType.includes('VARCHAR')) {
    return '#f9826c'; // Orange
  }
  if (upperType.includes('BOOL')) {
    return '#cca700'; // Yellow
  }

  return '#8c8c8c'; // Gray for others
}

// Format cell value for display
function formatCellValue(value: any): string {
  if (value === null || value === undefined) {
    return '<span class="null-value">NULL</span>';
  }

  if (typeof value === 'string') {
    return escapeHtml(value);
  }

  if (typeof value === 'object') {
    return escapeHtml(JSON.stringify(value));
  }

  return escapeHtml(String(value));
}

// Escape HTML to prevent XSS
function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}


// Render the interactive table
function renderTable(
  container: HTMLElement,
  data: FlinkTableData,
  state: TableState,
  updateState: (newState: Partial<TableState>) => void
): void {
  const { rows, columns } = data;

  if (!rows || rows.length === 0) {
    container.innerHTML = '<div class="empty-message">No data to display</div>';
    return;
  }

  // Get column names
  const columnNames = columns?.map(c => c.name) || Object.keys(rows[0]);

  // Pagination
  const totalRows = rows.length;
  const totalPages = Math.ceil(totalRows / state.rowsPerPage);
  const startIdx = state.currentPage * state.rowsPerPage;
  const endIdx = Math.min(startIdx + state.rowsPerPage, totalRows);
  const pageRows = rows.slice(startIdx, endIdx);

  // Build HTML
  const html = `
    <div class="flink-table-container">
      <!-- Toolbar -->
      <div class="table-toolbar">
        <div class="toolbar-left">
          <span class="row-count">
            ${totalRows.toLocaleString()} row${totalRows !== 1 ? 's' : ''}
          </span>
        </div>
        <div class="toolbar-right">
          <button class="copy-button" title="Copy to clipboard">
            <span class="icon">📋</span> Copy
          </button>
        </div>
      </div>

      <!-- Table -->
      <div class="table-wrapper">
        <table class="flink-table">
          <thead>
            <tr>
              ${columnNames.map(colName => {
                const column = columns?.find(c => c.name === colName);
                const type = column?.logicalType?.type || '';
                const displayType = getTypeDisplayName(type);
                const typeColor = getTypeColor(type);

                return `
                  <th>
                    <div class="th-content">
                      <span class="column-name">${escapeHtml(colName)}</span>
                      ${type ? `<span class="type-badge" style="background-color: ${typeColor}">${displayType}</span>` : ''}
                    </div>
                  </th>
                `;
              }).join('')}
            </tr>
          </thead>
          <tbody>
            ${pageRows.map((row, idx) => `
              <tr class="${idx % 2 === 0 ? 'even' : 'odd'}">
                ${columnNames.map(colName => `
                  <td>${formatCellValue(row[colName])}</td>
                `).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      ${totalPages > 1 ? `
        <div class="pagination">
          <button class="page-btn" data-action="first" ${state.currentPage === 0 ? 'disabled' : ''}>
            ⏮ First
          </button>
          <button class="page-btn" data-action="prev" ${state.currentPage === 0 ? 'disabled' : ''}>
            ← Previous
          </button>
          <span class="page-info">
            Page ${state.currentPage + 1} of ${totalPages}
            (showing ${startIdx + 1}-${endIdx} of ${totalRows.toLocaleString()})
          </span>
          <button class="page-btn" data-action="next" ${state.currentPage >= totalPages - 1 ? 'disabled' : ''}>
            Next →
          </button>
          <button class="page-btn" data-action="last" ${state.currentPage >= totalPages - 1 ? 'disabled' : ''}>
            Last ⏭
          </button>
        </div>
      ` : ''}
    </div>
  `;

  container.innerHTML = html;

  // Attach event listeners
  attachEventListeners(container, data, state, updateState);
}

// Attach event listeners to interactive elements
function attachEventListeners(
  container: HTMLElement,
  data: FlinkTableData,
  state: TableState,
  updateState: (newState: Partial<TableState>) => void
): void {

  // Pagination buttons
  const pageButtons = container.querySelectorAll('.page-btn[data-action]');
  pageButtons.forEach(button => {
    button.addEventListener('click', () => {
      const action = button.getAttribute('data-action');
      const totalPages = Math.ceil(data.rows.length / state.rowsPerPage);

      let newPage = state.currentPage;
      switch (action) {
        case 'first':
          newPage = 0;
          break;
        case 'prev':
          newPage = Math.max(0, state.currentPage - 1);
          break;
        case 'next':
          newPage = Math.min(totalPages - 1, state.currentPage + 1);
          break;
        case 'last':
          newPage = totalPages - 1;
          break;
      }

      updateState({ currentPage: newPage });
    });
  });

  // Copy button
  const copyButton = container.querySelector('.copy-button');
  if (copyButton) {
    copyButton.addEventListener('click', () => {
      copyTableToClipboard(data.rows, data.columns);
    });
  }
}

// Copy table data to clipboard as TSV
function copyTableToClipboard(rows: any[], columns?: ColumnInfo[]): void {
  const columnNames = columns?.map(c => c.name) || Object.keys(rows[0] || {});

  // Header row
  const lines = [columnNames.join('\t')];

  // Data rows
  rows.forEach(row => {
    const values = columnNames.map(col => {
      const value = row[col];
      if (value === null || value === undefined) return 'NULL';
      return String(value);
    });
    lines.push(values.join('\t'));
  });

  const text = lines.join('\n');
  navigator.clipboard.writeText(text).then(() => {
    // Show feedback (could enhance with a toast notification)
    const button = document.querySelector('.copy-button');
    if (button) {
      const originalText = button.innerHTML;
      button.innerHTML = '<span class="icon">✓</span> Copied!';
      setTimeout(() => {
        button.innerHTML = originalText;
      }, 2000);
    }
  }).catch(err => {
    console.error('Failed to copy:', err);
  });
}

// Inject CSS styles
function injectStyles(): void {
  if (document.getElementById('flink-table-styles')) return;

  const style = document.createElement('style');
  style.id = 'flink-table-styles';
  style.textContent = `
    .flink-table-container {
      font-family: var(--vscode-font-family);
      font-size: var(--vscode-font-size);
      color: var(--vscode-foreground);
      width: 100%;
      margin: 8px 0;
    }

    .table-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 8px;
      padding: 8px;
      background-color: var(--vscode-editor-background);
      border: 1px solid var(--vscode-panel-border);
      border-radius: 4px;
    }

    .toolbar-left, .toolbar-right {
      display: flex;
      align-items: center;
      gap: 12px;
    }


    .row-count {
      font-size: 12px;
      color: var(--vscode-descriptionForeground);
    }

    .copy-button {
      padding: 4px 12px;
      background-color: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border: none;
      border-radius: 3px;
      cursor: pointer;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 4px;
    }

    .copy-button:hover {
      background-color: var(--vscode-button-hoverBackground);
    }

    .copy-button .icon {
      font-size: 14px;
    }

    .table-wrapper {
      overflow-x: auto;
      border: 1px solid var(--vscode-panel-border);
      border-radius: 4px;
    }

    .flink-table {
      width: 100%;
      border-collapse: collapse;
      background-color: var(--vscode-editor-background);
    }

    .flink-table th {
      background-color: var(--vscode-list-hoverBackground);
      padding: 8px 12px;
      text-align: left;
      font-weight: 600;
      border-bottom: 2px solid var(--vscode-panel-border);
      position: sticky;
      top: 0;
      z-index: 10;
    }

    .th-content {
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .column-name {
      flex: 1;
    }

    .type-badge {
      font-size: 9px;
      font-weight: 700;
      padding: 2px 6px;
      border-radius: 3px;
      color: white;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .flink-table td {
      padding: 6px 12px;
      border-bottom: 1px solid var(--vscode-panel-border);
    }

    .flink-table tr.even {
      background-color: var(--vscode-editor-background);
    }

    .flink-table tr.odd {
      background-color: var(--vscode-list-hoverBackground);
      opacity: 0.5;
    }

    .flink-table tbody tr:hover {
      background-color: var(--vscode-list-hoverBackground) !important;
      opacity: 1;
    }

    .null-value {
      color: var(--vscode-descriptionForeground);
      font-style: italic;
      opacity: 0.7;
    }

    .pagination {
      display: flex;
      justify-content: center;
      align-items: center;
      gap: 8px;
      margin-top: 12px;
      padding: 8px;
    }

    .page-btn {
      padding: 4px 12px;
      background-color: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border);
      border-radius: 3px;
      cursor: pointer;
      font-size: 12px;
    }

    .page-btn:hover:not(:disabled) {
      background-color: var(--vscode-button-secondaryHoverBackground);
    }

    .page-btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    .page-info {
      font-size: 12px;
      color: var(--vscode-descriptionForeground);
      margin: 0 8px;
    }

    .empty-message {
      padding: 20px;
      text-align: center;
      color: var(--vscode-descriptionForeground);
      font-style: italic;
    }
  `;

  document.head.appendChild(style);
}

// Main activation function
const activateRenderer: ActivationFunction = (context: RendererContext) => {
  // Inject styles once
  injectStyles();

  return {
    renderOutputItem(outputItem: OutputItem, element: HTMLElement): void {
      try {
        // Parse the JSON data
        const data: FlinkTableData = outputItem.json();

        // Initialize or restore state
        let state: TableState = context.getState() || {
          currentPage: 0,
          rowsPerPage: 50
        };

        // State update function
        const updateState = (newState: Partial<TableState>) => {
          state = { ...state, ...newState };
          context.setState(state);
          renderTable(element, data, state, updateState);
        };

        // Initial render
        renderTable(element, data, state, updateState);

      } catch (error) {
        element.innerHTML = `
          <div style="color: var(--vscode-errorForeground); padding: 12px;">
            Error rendering table: ${error instanceof Error ? error.message : String(error)}
          </div>
        `;
      }
    }
  };
};

// Export the activate function for VSCode notebook renderer
// This needs to be a named export that VSCode can find
export { activateRenderer as activate };

// Also expose on globalThis as a fallback for direct script loading
if (typeof globalThis !== 'undefined') {
  (globalThis as any).activate = activateRenderer;
}
