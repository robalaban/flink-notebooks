/**
 * Main extension entry point
 */

import * as vscode from 'vscode';
import { ClusterManager, ClusterStatus } from './services/clusterManager';
import { SqlGatewayClient } from './services/sqlGatewayClient';
import { CatalogService } from './services/catalogService';
import { FlinkJobClient } from './services/flinkJobClient';
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
let statusBarItem: vscode.StatusBarItem;

export async function activate(context: vscode.ExtensionContext) {
  console.log('Flink Notebooks extension activating...');

  // Get configuration
  const config = vscode.workspace.getConfiguration('flink-notebooks');
  const gatewayPort = config.get<number>('gatewayPort', 8083);
  const javaPath = config.get<string>('javaPath');
  const jvmMemory = config.get<string>('jvmMemory');
  const parallelism = config.get<number>('parallelism');
  const taskSlots = config.get<number>('taskSlots');
  const jarPath = config.get<string>('miniclusterJarPath');

  // Initialize cluster manager
  clusterManager = new ClusterManager({
    javaPath: javaPath || undefined,
    jvmMemory: jvmMemory || undefined,
    parallelism: parallelism || undefined,
    taskSlots: taskSlots || undefined,
    gatewayPort,
    jarPath: jarPath || undefined,
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

export function deactivate() {
  console.log('Flink Notebooks extension deactivating...');

  // Clean up session
  if (sessionManager) {
    sessionManager.closeSession().catch(console.error);
  }
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

  // Stop streaming
  context.subscriptions.push(
    vscode.commands.registerCommand('flink-notebooks.stopStreaming', async (cell: vscode.NotebookCell) => {
      await notebookController.stopStreaming(cell);
      vscode.window.showInformationMessage('Streaming stopped');
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

    await clusterManager.stop();
    await sessionManager.closeSession();

    vscode.window.showInformationMessage('Flink cluster stopped');
    updateStatusBar('Stopped');

    // Disable catalog view
    vscode.commands.executeCommand('setContext', 'flink-notebooks:catalogAvailable', false);

    // Disable job monitor and stop auto-refresh
    vscode.commands.executeCommand('setContext', 'flink-notebooks:clusterRunning', false);
    jobMonitorProvider.stopAutoRefresh();
  } catch (error) {
    vscode.window.showErrorMessage(
      `Failed to stop cluster: ${error instanceof Error ? error.message : String(error)}`
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
