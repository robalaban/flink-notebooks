/**
 * Serializer for Flink Notebook files (.flinknb)
 */

import * as vscode from 'vscode';

interface RawNotebook {
  cells: RawCell[];
  metadata?: Record<string, unknown>;
}

interface RawCell {
  language: string;
  value: string;
  kind: vscode.NotebookCellKind;
  metadata?: Record<string, unknown>;
  outputs?: RawCellOutput[];
}

interface RawCellOutput {
  items: { mime: string; data: string }[];
}

export class FlinkNotebookSerializer implements vscode.NotebookSerializer {
  public readonly label: string = 'Flink Notebook Serializer';

  async deserializeNotebook(
    content: Uint8Array,
    _token: vscode.CancellationToken
  ): Promise<vscode.NotebookData> {
    const contents = Buffer.from(content).toString('utf8');

    let raw: RawNotebook;
    try {
      raw = JSON.parse(contents);
    } catch {
      raw = { cells: [] };
    }

    const cells = raw.cells.map((cell) => {
      const cellData = new vscode.NotebookCellData(
        cell.kind ?? vscode.NotebookCellKind.Code,
        cell.value,
        cell.language ?? 'flink-sql'
      );

      cellData.metadata = cell.metadata;

      if (cell.outputs) {
        cellData.outputs = cell.outputs.map((output) => {
          const items = output.items.map(
            (item) =>
              new vscode.NotebookCellOutputItem(Buffer.from(item.data, 'base64'), item.mime)
          );
          return new vscode.NotebookCellOutput(items);
        });
      }

      return cellData;
    });

    const notebookData = new vscode.NotebookData(cells);
    notebookData.metadata = raw.metadata;

    return notebookData;
  }

  async serializeNotebook(
    data: vscode.NotebookData,
    _token: vscode.CancellationToken
  ): Promise<Uint8Array> {
    const cells: RawCell[] = [];

    for (const cell of data.cells) {
      const outputs: RawCellOutput[] = [];

      for (const output of cell.outputs ?? []) {
        const items = output.items.map((item) => ({
          mime: item.mime,
          data: Buffer.from(item.data).toString('base64'),
        }));
        outputs.push({ items });
      }

      cells.push({
        kind: cell.kind,
        language: cell.languageId,
        value: cell.value,
        metadata: cell.metadata,
        outputs: outputs.length > 0 ? outputs : undefined,
      });
    }

    const raw: RawNotebook = {
      cells,
      metadata: data.metadata,
    };

    return Buffer.from(JSON.stringify(raw, null, 2), 'utf8');
  }
}
