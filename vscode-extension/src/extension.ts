/**
 * Main extension entry point
 */

import * as vscode from 'vscode';
import { ClusterManager, ClusterStatus } from './services/clusterManager';
import { SqlGatewayClient } from './services/sqlGatewayClient';
import { CatalogService } from './services/catalogService';
import { FlinkJobClient } from './services/flinkJobClient';
import { UdfManager } from './services/udfManager';
import {
  FlinkNotebookController,
  SessionManager,
} from './providers/flinkNotebookController';
import { FlinkNotebookSerializer } from './providers/flinkNotebookSerializer';
import { CatalogTreeProvider } from './providers/catalogTreeProvider';
import { JobMonitorProvider } from './providers/jobMonitorProvider';

let clusterManager: ClusterManager;
let gatewayClient: SqlGatewayClient;
let jobClient: FlinkJobClient;
let catalogService: CatalogService;
let sessionManager: SessionManager;
let notebookController: FlinkNotebookController;
let catalogTreeProvider: CatalogTreeProvider;
let jobMonitorProvider: JobMonitorProvider;
let udfManager: UdfManager;
let statusBarItem: vscode.StatusBarItem;
let outputChannel: vscode.OutputChannel;

export async function activate(context: vscode.ExtensionContext) {
  console.log('Flink Notebooks extension activating...');

  // Create output channel for extension logs
  outputChannel = vscode.window.createOutputChannel('Flink Notebooks');
  context.subscriptions.push(outputChannel);
  outputChannel.appendLine('Flink Notebooks extension activating...');

  // Get configuration
  const config = vscode.workspace.getConfiguration('flink-notebooks');
  const gatewayPort = config.get<number>('gatewayPort', 8083);
  const javaPath = config.get<string>('javaPath');
  const jvmMemory = config.get<string>('jvmMemory');
  const parallelism = config.get<number>('parallelism');
  const taskSlots = config.get<number>('taskSlots');
  const jarPath = config.get<string>('miniclusterJarPath');
  const connectorLibraryPath = config.get<string>('connectorLibraryPath');

  // Initialize cluster manager
  clusterManager = new ClusterManager({
    javaPath: javaPath || undefined,
    jvmMemory: jvmMemory || undefined,
    parallelism: parallelism || undefined,
    taskSlots: taskSlots || undefined,
    gatewayPort,
    jarPath: jarPath || undefined,
    connectorLibraryPath: connectorLibraryPath || undefined,
  });

  // Set up logger to route cluster logs to output channel
  clusterManager.setLogger({
    log: (message: string) => outputChannel.appendLine(message),
    error: (message: string) => outputChannel.appendLine(`[ERROR] ${message}`),
  });

  // Initialize SQL Gateway client
  gatewayClient = new SqlGatewayClient(`http://localhost:${gatewayPort}`);

  // Initialize Flink Job REST client
  jobClient = new FlinkJobClient('http://localhost:8081');

  // Initialize session manager
  sessionManager = new SessionManager(gatewayClient);

  // Initialize catalog service (uses Flink SQL Gateway to query catalogs)
  catalogService = new CatalogService(
    gatewayClient,
    () => sessionManager.getOrCreateSession()
  );

  // Initialize UDF manager
  udfManager = new UdfManager();
  udfManager.setLogger({
    log: (message: string) => outputChannel.appendLine(message),
    error: (message: string) => outputChannel.appendLine(`[ERROR] ${message}`),
  });

  // Scan for existing UDFs (gracefully handle if no workspace)
  try {
    await udfManager.scanUdfs();
  } catch (error) {
    // Ignore errors during scan - UDFs are optional
    console.log('UDF scan skipped:', error);
  }

  // Set up auto-registration of UDFs when session is created
  const udfConfig = vscode.workspace.getConfiguration('flink-notebooks');
  const udfAutoRegister = udfConfig.get<boolean>('udfAutoRegister', true);

  if (udfAutoRegister) {
    sessionManager.setOnSessionCreated(async (sessionHandle) => {
      outputChannel.appendLine('Auto-registering UDFs in new session...');
      try {
        await udfManager.registerAllUdfs(gatewayClient, sessionHandle);
        const udfs = udfManager.getRegisteredUdfs();
        const registeredCount = udfs.filter((u) => u.registered).length;
        if (registeredCount > 0) {
          outputChannel.appendLine(`Auto-registered ${registeredCount} UDF(s) successfully`);
        }
      } catch (error) {
        outputChannel.appendLine(
          `[ERROR] Failed to auto-register UDFs: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    });
  }

  // Register notebook serializer
  context.subscriptions.push(
    vscode.workspace.registerNotebookSerializer(
      'flink-notebook',
      new FlinkNotebookSerializer()
    )
  );

  // Register notebook controller
  notebookController = new FlinkNotebookController(gatewayClient, sessionManager);
  context.subscriptions.push(notebookController);

  // Register catalog tree provider (with auto-refresh on catalog changes)
  catalogTreeProvider = new CatalogTreeProvider(catalogService, notebookController);
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('flinkCatalog', catalogTreeProvider)
  );

  // Register job monitor provider (with auto-refresh)
  const jobRefreshInterval = config.get<number>('jobRefreshInterval', 5000);
  jobMonitorProvider = new JobMonitorProvider(
    jobClient,
    { refreshInterval: jobRefreshInterval },
    notebookController
  );
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('flinkJobs', jobMonitorProvider)
  );
  context.subscriptions.push(jobMonitorProvider);

  // Create status bar item
  statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
  statusBarItem.command = 'flink-notebooks.showClusterStatus';
  context.subscriptions.push(statusBarItem);

  // Register crash handler to notify user when cluster crashes
  // Must be after statusBarItem is created so we can update it
  clusterManager.onCrash((exitCode) => {
    const message = exitCode !== null
      ? `Flink cluster crashed unexpectedly with exit code ${exitCode}.`
      : 'Flink cluster encountered an error and stopped.';

    // Log to output channel
    outputChannel.appendLine(`[ERROR] ${message}`);
    outputChannel.appendLine('Check the Flink Notebooks output channel for details.');

    // Update status bar immediately to show crashed state
    updateStatusBar('Crashed');

    vscode.window.showErrorMessage(message, 'View Logs', 'Restart Cluster').then((choice) => {
      if (choice === 'View Logs') {
        outputChannel.show();
      } else if (choice === 'Restart Cluster') {
        vscode.commands.executeCommand('flink-notebooks.startCluster');
      }
    });

    // Update other UI components if they exist
    if (jobMonitorProvider) {
      jobMonitorProvider.refresh();
    }
    if (catalogTreeProvider) {
      // Suppress notifications temporarily to avoid double-notification
      catalogTreeProvider.setSuppressNotifications(true, 2000);
      catalogTreeProvider.refresh();
    }
  });

  // Register commands
  registerCommands(context);

  // Auto-start cluster if configured
  const autoStart = vscode.workspace
    .getConfiguration('flink-notebooks')
    .get('autoStartCluster', true);

  if (autoStart) {
    const notebooks = vscode.workspace.notebookDocuments.filter(
      (doc) => doc.notebookType === 'flink-notebook'
    );
    if (notebooks.length > 0) {
      await startCluster();
    }
  }

  // Update status bar
  updateStatusBar();

  console.log('Flink Notebooks extension activated');
}

export async function deactivate() {
  console.log('Flink Notebooks extension deactivating...');

  // Clean up session
  if (sessionManager) {
    try {
      await sessionManager.closeSession();
      console.log('Session closed successfully');
    } catch (error) {
      console.error('Error closing session:', error);
    }
  }

  // Stop the cluster to prevent orphaned Java processes
  if (clusterManager) {
    try {
      await clusterManager.stop();
      console.log('Cluster stopped successfully');
    } catch (error) {
      console.error('Error stopping cluster:', error);
    }
  }

  console.log('Flink Notebooks extension deactivated');
}

function registerCommands(context: vscode.ExtensionContext) {
  // New notebook command
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.newNotebook', async () => {
      const document = await vscode.workspace.openNotebookDocument('flink-notebook', {
        cells: [],
        metadata: {
          custom: {
            created_at: new Date().toISOString(),
          },
        },
      });

      await vscode.window.showNotebookDocument(document);
    })
  );

  // Start cluster command
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.startCluster', async () => {
      await startCluster();
    })
  );

  // Stop cluster command
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.stopCluster', async () => {
      await stopCluster();
    })
  );

  // Restart cluster command
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.restartCluster', async () => {
      await restartCluster();
    })
  );

  // Show cluster status
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.showClusterStatus', async () => {
      try {
        const status = clusterManager.getStatus();
        const pid = clusterManager.getPid();
        const gatewayUrl = clusterManager.getGatewayUrl();
        const message = `Cluster Status: ${status}\nPID: ${pid || 'N/A'}\nGateway: ${gatewayUrl}`;
        vscode.window.showInformationMessage(message);
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to get cluster status: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );

  // Refresh catalog
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.refreshCatalog', () => {
      catalogTreeProvider.refresh();
    })
  );

  // Insert table reference
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.insertTable', (item) => {
      catalogTreeProvider.insertTableReference(item);
    })
  );

  // Pause streaming
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.pauseStreaming', async (cell: vscode.NotebookCell) => {
      try {
        await notebookController.pauseStreaming(cell);
        vscode.window.showInformationMessage('Streaming paused');
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to pause streaming: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );

  // Resume streaming
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.resumeStreaming', async (cell: vscode.NotebookCell) => {
      try {
        await notebookController.resumeStreaming(cell);
        vscode.window.showInformationMessage('Streaming resumed');
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to resume streaming: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );

  // Cancel operation
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.cancelOperation', async (cell: vscode.NotebookCell) => {
      const confirmation = await vscode.window.showWarningMessage(
        'Cancel this operation? The job will be stopped and cannot be resumed.',
        { modal: true },
        'Cancel Job'
      );

      if (confirmation === 'Cancel Job') {
        try {
          await notebookController.cancelOperation(cell);
          vscode.window.showInformationMessage('Operation canceled successfully');
        } catch (error) {
          vscode.window.showErrorMessage(
            `Failed to cancel operation: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    })
  );

  // Clear notebook outputs
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.clearNotebookOutputs', async () => {
      const editor = vscode.window.activeNotebookEditor;
      if (!editor) {
        vscode.window.showWarningMessage('No active notebook found');
        return;
      }

      const notebook = editor.notebook;
      if (notebook.notebookType !== 'flink-notebook') {
        vscode.window.showWarningMessage('Active notebook is not a Flink notebook');
        return;
      }

      const confirmation = await vscode.window.showWarningMessage(
        `Clear all outputs from "${notebook.uri.fsPath.split('/').pop()}"?\n\nThis will remove all cell outputs, making the notebook safe for version control.`,
        { modal: true },
        'Clear Outputs'
      );

      if (confirmation === 'Clear Outputs') {
        try {
          const edit = new vscode.WorkspaceEdit();
          const edits: vscode.NotebookEdit[] = [];

          // Clear outputs for all cells
          for (let i = 0; i < notebook.cellCount; i++) {
            const cell = notebook.cellAt(i);
            if (cell.outputs.length > 0) {
              // Replace the cell with same content but no outputs
              const newCell = new vscode.NotebookCellData(
                cell.kind,
                cell.document.getText(),
                cell.document.languageId
              );
              newCell.metadata = cell.metadata;
              newCell.outputs = []; // Clear outputs

              edits.push(
                vscode.NotebookEdit.replaceCells(
                  new vscode.NotebookRange(i, i + 1),
                  [newCell]
                )
              );
            }
          }

          edit.set(notebook.uri, edits);
          const success = await vscode.workspace.applyEdit(edit);

          if (success) {
            vscode.window.showInformationMessage(
              `Cleared outputs from ${notebook.cellCount} cells. Ready for GitHub!`
            );
          } else {
            vscode.window.showErrorMessage('Failed to clear outputs');
          }
        } catch (error) {
          vscode.window.showErrorMessage(
            `Failed to clear outputs: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    })
  );

  // Job monitoring commands
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.refreshJobs', () => {
      jobMonitorProvider.refresh();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.openFlinkWebUI', () => {
      const url = jobClient.getWebUIUrl();
      vscode.env.openExternal(vscode.Uri.parse(url));
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.openJobInWebUI', async (item: any) => {
      // Extract job ID from tree item or direct parameter
      const jobId = typeof item === 'string' ? item : item?.job?.jid;
      if (!jobId) {
        vscode.window.showErrorMessage('Could not determine job ID');
        return;
      }
      const url = jobClient.getJobWebUIUrl(jobId);
      vscode.env.openExternal(vscode.Uri.parse(url));
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.viewJobDetails', async (item: any) => {
      // Extract job ID from tree item or direct parameter
      const jobId = typeof item === 'string' ? item : item?.job?.jid;
      if (!jobId) {
        vscode.window.showErrorMessage('Could not determine job ID');
        return;
      }
      const details = await jobMonitorProvider.getJobDetailsText(jobId);
      const outputChannel = vscode.window.createOutputChannel('Flink Job Details');
      outputChannel.clear();
      outputChannel.appendLine(details);
      outputChannel.show();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.viewJobMetrics', async (item: any) => {
      // Extract job ID from tree item or direct parameter
      const jobId = typeof item === 'string' ? item : item?.job?.jid;
      if (!jobId) {
        vscode.window.showErrorMessage('Could not determine job ID');
        return;
      }
      const metrics = await jobMonitorProvider.getJobMetricsText(jobId);
      const outputChannel = vscode.window.createOutputChannel('Flink Job Metrics');
      outputChannel.clear();
      outputChannel.appendLine(metrics);
      outputChannel.show();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.cancelJob', async (item: any) => {
      // Extract job info from tree item or direct parameter
      const jobId = typeof item === 'string' ? item : item?.job?.jid;
      const jobName = typeof item === 'string' ? item : item?.job?.name || jobId;

      if (!jobId) {
        vscode.window.showErrorMessage('Could not determine job ID');
        return;
      }

      const confirmation = await vscode.window.showWarningMessage(
        `Are you sure you want to cancel job "${jobName}"?\n\nJob ID: ${jobId}`,
        { modal: true },
        'Cancel Job'
      );

      if (confirmation === 'Cancel Job') {
        try {
          // First, stop any cells that are streaming this job
          await notebookController.stopStreamingByJobId(jobId);

          // Then cancel the job via Flink REST API
          await jobClient.cancelJob(jobId);
          vscode.window.showInformationMessage(`Job "${jobName}" canceled successfully`);
          jobMonitorProvider.refresh();
        } catch (error) {
          vscode.window.showErrorMessage(
            `Failed to cancel job "${jobName}": ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    })
  );

  // UDF Commands

  // Create UDF
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.createUdf', async () => {
      try {
        // Ask for UDF type
        const udfType = await vscode.window.showQuickPick(
          [
            { label: 'Scalar Function', value: 'scalar', description: 'Map input values to a single output value' },
            { label: 'Table Function', value: 'table', description: 'Map input values to multiple rows' },
            { label: 'Aggregate Function', value: 'aggregate', description: 'Aggregate multiple rows into a single value' },
          ],
          { placeHolder: 'Select UDF type' }
        );

        if (!udfType) {
          return;
        }

        // Ask for class name
        const className = await vscode.window.showInputBox({
          prompt: 'Enter UDF class name (e.g., MyUpperCase)',
          placeHolder: 'MyUpperCase',
          validateInput: (value) => {
            if (!value) {
              return 'Class name is required';
            }
            if (!/^[A-Z][a-zA-Z0-9]*$/.test(value)) {
              return 'Class name must start with uppercase letter and contain only alphanumeric characters';
            }
            return null;
          },
        });

        if (!className) {
          return;
        }

        // Ask for function name (SQL identifier)
        const functionName = await vscode.window.showInputBox({
          prompt: 'Enter function name for SQL (e.g., my_upper)',
          placeHolder: 'my_upper',
          value: className.replace(/([A-Z])/g, '_$1').toLowerCase().substring(1),
          validateInput: (value) => {
            if (!value) {
              return 'Function name is required';
            }
            if (!/^[a-z][a-z0-9_]*$/.test(value)) {
              return 'Function name must start with lowercase letter and contain only lowercase letters, numbers, and underscores';
            }
            return null;
          },
        });

        if (!functionName) {
          return;
        }

        // Ask for description (optional)
        const description = await vscode.window.showInputBox({
          prompt: 'Enter function description (optional)',
          placeHolder: 'Converts string to uppercase',
        });

        // Create UDF file
        const filePath = await udfManager.createUdf(
          className,
          functionName,
          udfType.value as any,
          description || undefined
        );

        vscode.window.showInformationMessage(`Created UDF ${className} at ${filePath}`);

        // Open the file
        const document = await vscode.workspace.openTextDocument(filePath);
        await vscode.window.showTextDocument(document);

        // Ask if user wants to build now
        const buildNow = await vscode.window.showInformationMessage(
          'Would you like to build the UDFs now?',
          'Build',
          'Later'
        );

        if (buildNow === 'Build') {
          await vscode.commands.executeCommand('flink-notebooks.buildUdfs');
        }
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to create UDF: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );

  // Build UDFs
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.buildUdfs', async () => {
      try {
        vscode.window.showInformationMessage('Building UDFs...');
        await udfManager.buildUdfs();

        // Check if cluster is running
        const clusterStatus = clusterManager.getStatus();
        const isRunning = clusterStatus === 'running';

        // Offer to restart cluster if it's running
        if (isRunning) {
          const action = await vscode.window.showInformationMessage(
            'UDFs built successfully! Restart cluster to load the new UDFs.',
            'Restart Cluster',
            'Later'
          );

          if (action === 'Restart Cluster') {
            await vscode.commands.executeCommand('flink-notebooks.restartCluster');
          }
        } else {
          vscode.window.showInformationMessage(
            'UDFs built successfully! Start the cluster to load them.'
          );
        }
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to build UDFs: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );

  // Register UDFs
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.registerUdfs', async () => {
      try {
        const sessionHandle = await sessionManager.getOrCreateSession();
        vscode.window.showInformationMessage('Registering UDFs...');
        await udfManager.registerAllUdfs(gatewayClient, sessionHandle);
        const udfs = udfManager.getRegisteredUdfs();
        vscode.window.showInformationMessage(
          `Registered ${udfs.filter((u) => u.registered).length} UDF(s) successfully!`
        );
      } catch (error) {
        vscode.window.showErrorMessage(
          `Failed to register UDFs: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    })
  );
}

async function startCluster(): Promise<void> {
  try {
    updateStatusBar('Starting...', true);

    const config = vscode.workspace.getConfiguration('flink-notebooks');
    const memory = config.get<string>('jvmMemory');
    const parallelism = config.get<number>('parallelism');

    await clusterManager.start(memory, parallelism);

    vscode.window.showInformationMessage('Flink cluster started successfully');
    updateStatusBar('Running');

    // Enable catalog view
    vscode.commands.executeCommand('setContext', 'flink-notebooks:catalogAvailable', true);

    // Refresh catalog tree to show available catalogs
    if (catalogTreeProvider) {
      catalogTreeProvider.refresh();
    }

    // Enable job monitor and start auto-refresh
    vscode.commands.executeCommand('setContext', 'flink-notebooks:clusterRunning', true);
    jobMonitorProvider.startAutoRefresh();
  } catch (error) {
    vscode.window.showErrorMessage(
      `Failed to start cluster: ${error instanceof Error ? error.message : String(error)}`
    );
    updateStatusBar('Error');
  }
}

async function stopCluster(): Promise<void> {
  try {
    updateStatusBar('Stopping...', true);

    // Stop the cluster first
    await clusterManager.stop();

    // Try to close the session, but don't fail if it's already gone
    try {
      await sessionManager.closeSession();
    } catch (sessionError) {
      // Session might already be closed due to cluster shutdown, ignore
      console.log('Session close failed (expected after cluster stop):', sessionError);
    }

    vscode.window.showInformationMessage('Flink cluster stopped');
    updateStatusBar('Stopped');

    // Disable catalog view
    vscode.commands.executeCommand('setContext', 'flink-notebooks:catalogAvailable', false);

    // Refresh catalog tree to show stopped state
    if (catalogTreeProvider) {
      catalogTreeProvider.refresh();
    }

    // Disable job monitor and stop auto-refresh
    vscode.commands.executeCommand('setContext', 'flink-notebooks:clusterRunning', false);
    jobMonitorProvider.stopAutoRefresh();
  } catch (error) {
    vscode.window.showErrorMessage(
      `Failed to stop cluster: ${error instanceof Error ? error.message : String(error)}`
    );
    updateStatusBar('Stopped'); // Still update to stopped even if there was an error

    // Still clean up UI state
    vscode.commands.executeCommand('setContext', 'flink-notebooks:catalogAvailable', false);
    vscode.commands.executeCommand('setContext', 'flink-notebooks:clusterRunning', false);
    jobMonitorProvider.stopAutoRefresh();
    if (catalogTreeProvider) {
      catalogTreeProvider.refresh();
    }
  }
}

async function restartCluster(): Promise<void> {
  try {
    vscode.window.showInformationMessage('Restarting Flink cluster...');

    // Stop the cluster
    await stopCluster();

    // Wait a moment for cleanup
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Start the cluster
    await startCluster();

    vscode.window.showInformationMessage('Flink cluster restarted successfully');
  } catch (error) {
    vscode.window.showErrorMessage(
      `Failed to restart cluster: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

function updateStatusBar(status?: string, loading: boolean = false): void {
  if (status) {
    statusBarItem.text = loading ? `$(sync~spin) Flink: ${status}` : `$(database) Flink: ${status}`;
  } else {
    statusBarItem.text = '$(database) Flink';
  }

  statusBarItem.show();
}
