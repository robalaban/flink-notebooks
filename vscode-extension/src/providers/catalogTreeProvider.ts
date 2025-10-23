/**
 * Tree view provider for Flink Catalog
 */

import * as vscode from 'vscode';
import { CatalogService, CatalogNode } from '../services/catalogService';
import { FlinkNotebookController } from './flinkNotebookController';

export class CatalogTreeProvider implements vscode.TreeDataProvider<CatalogTreeItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<CatalogTreeItem | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private catalogTree: CatalogNode[] | null = null;
  private suppressNotifications: boolean = false;

  constructor(
    private catalogService: CatalogService,
    notebookController?: FlinkNotebookController
  ) {
    // Listen for statement executions to auto-refresh catalog when needed
    if (notebookController) {
      notebookController.onStatementExecuted((event) => {
        if (event.success && this.shouldRefreshCatalog(event.sql)) {
          console.log('Catalog-modifying statement detected, refreshing catalog tree');
          this.refresh();
        }
      });
    }
  }

  /**
   * Suppress notifications for a brief period (e.g., after cluster crash)
   */
  setSuppressNotifications(suppress: boolean, durationMs: number = 2000): void {
    this.suppressNotifications = suppress;
    if (suppress && durationMs > 0) {
      setTimeout(() => {
        this.suppressNotifications = false;
      }, durationMs);
    }
  }

  /**
   * Check if a SQL statement should trigger a catalog refresh
   */
  private shouldRefreshCatalog(sql: string): boolean {
    const upperSql = sql.toUpperCase();

    // Catalog operations
    if (upperSql.includes('CREATE CATALOG') ||
        upperSql.includes('DROP CATALOG') ||
        upperSql.includes('ALTER CATALOG')) {
      return true;
    }

    // Database operations
    if (upperSql.includes('CREATE DATABASE') ||
        upperSql.includes('DROP DATABASE') ||
        upperSql.includes('ALTER DATABASE')) {
      return true;
    }

    // Table operations
    if (upperSql.includes('CREATE TABLE') ||
        upperSql.includes('DROP TABLE') ||
        upperSql.includes('ALTER TABLE')) {
      return true;
    }

    return false;
  }

  refresh(): void {
    this.catalogTree = null;
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: CatalogTreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: CatalogTreeItem): Promise<CatalogTreeItem[]> {
    if (!element) {
      // Root level - fetch catalog tree
      try {
        // Check if Flink is available
        const isAvailable = await this.catalogService.isAvailable();
        if (!isAvailable) {
          // Only show notification if not suppressed (e.g., not right after a crash)
          if (!this.suppressNotifications) {
            vscode.window.showWarningMessage(
              'Flink cluster is not running. Start the cluster to view catalogs.',
              'Start Cluster'
            ).then((action) => {
              if (action === 'Start Cluster') {
                vscode.commands.executeCommand('flink-notebooks.startCluster');
              }
            });
          }

          return this.createEmptyState('Flink cluster not running');
        }

        // Fetch catalog tree if not cached
        if (!this.catalogTree) {
          this.catalogTree = await this.catalogService.getCatalogTree();
        }

        if (!this.catalogTree || this.catalogTree.length === 0) {
          return this.createEmptyState('No catalogs found');
        }

        // Return catalog nodes
        return this.catalogTree.map(
          (catalog) =>
            new CatalogTreeItem(
              catalog.name,
              catalog.type,
              catalog,
              catalog.children && catalog.children.length > 0
                ? vscode.TreeItemCollapsibleState.Collapsed
                : vscode.TreeItemCollapsibleState.None
            )
        );
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);

        // Only show notification if not suppressed (e.g., not right after a crash)
        if (!this.suppressNotifications) {
          vscode.window.showErrorMessage(
            `Failed to load catalogs: ${errorMessage}`,
            'Retry',
            'Start Cluster'
          ).then((action) => {
            if (action === 'Retry') {
              this.refresh();
            } else if (action === 'Start Cluster') {
              vscode.commands.executeCommand('flink-notebooks.startCluster');
            }
          });
        }

        return this.createEmptyState('Error loading catalogs');
      }
    }

    // Return children of the current node
    if (!element.node.children || element.node.children.length === 0) {
      return [];
    }

    return element.node.children.map(
      (child) =>
        new CatalogTreeItem(
          child.name,
          child.type,
          child,
          child.children && child.children.length > 0
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None
        )
    );
  }

  private createEmptyState(message: string): CatalogTreeItem[] {
    const emptyNode: CatalogNode = {
      type: 'catalog',
      name: message,
      metadata: {},
    };

    const item = new CatalogTreeItem(
      message,
      'catalog',
      emptyNode,
      vscode.TreeItemCollapsibleState.None
    );

    item.iconPath = new vscode.ThemeIcon('info');
    item.tooltip = message;

    return [item];
  }

  async insertTableReference(item: CatalogTreeItem): Promise<void> {
    if (item.nodeType !== 'table') {
      return;
    }

    // Get catalog and database from metadata
    const catalog = item.node.metadata?.catalog;
    const database = item.node.metadata?.database;
    const table = item.label;

    if (!catalog || !database || !table) {
      vscode.window.showErrorMessage('Unable to determine table path');
      return;
    }

    const qualifiedName = `\`${catalog}\`.\`${database}\`.\`${table}\``;
    const sqlSnippet = `SELECT * FROM ${qualifiedName} LIMIT 50`;

    // Insert into active editor or notebook cell
    const editor = vscode.window.activeTextEditor;
    if (editor) {
      editor.edit((editBuilder) => {
        editBuilder.insert(editor.selection.active, sqlSnippet);
      });
    } else {
      // Try to insert into active notebook cell
      const notebook = vscode.window.activeNotebookEditor;
      if (notebook) {
        const cell = notebook.notebook.cellAt(notebook.selection.start);
        const edit = new vscode.WorkspaceEdit();
        const range = new vscode.Range(
          cell.document.lineCount,
          0,
          cell.document.lineCount,
          0
        );
        edit.replace(cell.document.uri, range, sqlSnippet);
        await vscode.workspace.applyEdit(edit);
      } else {
        // Show message with SQL to copy
        vscode.window.showInformationMessage(`SQL: ${sqlSnippet}`, 'Copy').then((selection) => {
          if (selection === 'Copy') {
            vscode.env.clipboard.writeText(sqlSnippet);
          }
        });
      }
    }
  }
}

export class CatalogTreeItem extends vscode.TreeItem {
  constructor(
    public readonly label: string,
    public readonly nodeType: 'catalog' | 'database' | 'table',
    public readonly node: CatalogNode,
    public readonly collapsibleState: vscode.TreeItemCollapsibleState
  ) {
    super(label, collapsibleState);

    this.contextValue = nodeType;

    // Set icons based on node type
    switch (nodeType) {
      case 'catalog':
        this.iconPath = new vscode.ThemeIcon('database');
        break;
      case 'database':
        this.iconPath = new vscode.ThemeIcon('folder');
        break;
      case 'table':
        this.iconPath = new vscode.ThemeIcon('table');
        this.tooltip = this.getTooltip();
        break;
    }
  }

  private getTooltip(): string {
    const metadata = this.node.metadata;
    const parts: string[] = [];

    if (metadata?.catalog && metadata?.database) {
      parts.push(`${metadata.catalog}.${metadata.database}.${this.label}`);
    } else {
      parts.push(this.label);
    }

    if (metadata?.table_type) {
      parts.push(`Type: ${metadata.table_type}`);
    }

    return parts.join('\n');
  }
}
