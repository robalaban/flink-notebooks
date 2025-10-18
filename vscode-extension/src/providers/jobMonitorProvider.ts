/**
 * Tree view provider for Flink Job Monitoring
 */

import * as vscode from 'vscode';
import { FlinkJobClient, JobSummary, JobState, JobDetails } from '../services/flinkJobClient';
import { FlinkNotebookController } from './flinkNotebookController';

type JobTreeItemType = 'category' | 'job';

export class JobMonitorProvider implements vscode.TreeDataProvider<JobTreeItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<JobTreeItem | undefined | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private refreshInterval: NodeJS.Timeout | null = null;
  private jobs: JobSummary[] = [];

  constructor(
    private jobClient: FlinkJobClient,
    private config: { refreshInterval: number },
    notebookController?: FlinkNotebookController
  ) {
    // Listen for statement executions to trigger immediate refresh on job submissions
    if (notebookController) {
      notebookController.onStatementExecuted((event) => {
        if (event.success && this.shouldRefreshJobs(event.sql)) {
          console.log('Job-related statement detected, refreshing job list');
          this.refresh();
        }
      });
    }
  }

  /**
   * Check if a SQL statement should trigger a job refresh
   */
  private shouldRefreshJobs(sql: string): boolean {
    const upperSql = sql.toUpperCase();

    // INSERT statements create jobs
    if (upperSql.includes('INSERT INTO') || upperSql.includes('INSERT OVERWRITE')) {
      return true;
    }

    // SELECT statements with sinks create jobs
    if (upperSql.includes('EXECUTE')) {
      return true;
    }

    return false;
  }

  /**
   * Start auto-refresh
   */
  startAutoRefresh(): void {
    if (this.refreshInterval) {
      return; // Already running
    }

    this.refreshInterval = setInterval(() => {
      this.refresh();
    }, this.config.refreshInterval);

    // Initial refresh
    this.refresh();
  }

  /**
   * Stop auto-refresh
   */
  stopAutoRefresh(): void {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
  }

  /**
   * Manual refresh
   */
  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: JobTreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: JobTreeItem): Promise<JobTreeItem[]> {
    if (!element) {
      // Root level - fetch jobs and group by status
      try {
        const isAvailable = await this.jobClient.isAvailable();
        if (!isAvailable) {
          return this.createEmptyState('Flink cluster not running');
        }

        const overview = await this.jobClient.getJobOverview();
        this.jobs = overview.jobs;

        if (this.jobs.length === 0) {
          return this.createEmptyState('No jobs found');
        }

        // Group jobs by status
        const grouped = this.groupJobsByState(this.jobs);

        const categories: JobTreeItem[] = [];

        // Add categories with jobs
        const categoryOrder: JobState[] = ['RUNNING', 'FINISHED', 'FAILED', 'CANCELED', 'FAILING', 'CANCELLING'];

        for (const state of categoryOrder) {
          const jobsInState = grouped.get(state);
          if (jobsInState && jobsInState.length > 0) {
            categories.push(
              new JobTreeItem(
                `${this.getStateLabel(state)} (${jobsInState.length})`,
                'category',
                vscode.TreeItemCollapsibleState.Expanded,
                undefined,
                state,
                jobsInState
              )
            );
          }
        }

        return categories;
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        vscode.window.showErrorMessage(`Failed to load jobs: ${errorMessage}`);
        return this.createEmptyState('Error loading jobs');
      }
    }

    // Return jobs under a category
    if (element.itemType === 'category' && element.jobs) {
      return element.jobs.map((job) => this.createJobTreeItem(job));
    }

    return [];
  }

  private groupJobsByState(jobs: JobSummary[]): Map<JobState, JobSummary[]> {
    const grouped = new Map<JobState, JobSummary[]>();

    for (const job of jobs) {
      if (!grouped.has(job.state)) {
        grouped.set(job.state, []);
      }
      grouped.get(job.state)!.push(job);
    }

    return grouped;
  }

  private createJobTreeItem(job: JobSummary): JobTreeItem {
    const duration = this.formatDuration(job.duration);
    const label = `${job.name} (${duration})`;

    const item = new JobTreeItem(
      label,
      'job',
      vscode.TreeItemCollapsibleState.None,
      job
    );

    item.description = this.getJobDescription(job);
    item.tooltip = this.getJobTooltip(job);
    item.iconPath = this.getJobIcon(job.state);

    // Set context value based on state for context menu
    item.contextValue = job.state === 'RUNNING' ? 'job-running' : 'job';

    // Add command to open job details
    item.command = {
      command: 'flink-notebooks.viewJobDetails',
      title: 'View Job Details',
      arguments: [job.jid],
    };

    return item;
  }

  private getStateLabel(state: JobState): string {
    const labels: Record<JobState, string> = {
      RUNNING: '🟢 Running',
      FINISHED: '✅ Finished',
      FAILED: '❌ Failed',
      CANCELED: '⭕ Canceled',
      FAILING: '⚠️ Failing',
      CANCELLING: '🔄 Cancelling',
      CREATED: '⚪ Created',
      RESTARTING: '🔄 Restarting',
      SUSPENDED: '⏸️ Suspended',
      RECONCILING: '🔄 Reconciling',
    };

    return labels[state] || state;
  }

  private getJobIcon(state: JobState): vscode.ThemeIcon {
    const iconMap: Record<JobState, string> = {
      RUNNING: 'play-circle',
      FINISHED: 'check',
      FAILED: 'error',
      CANCELED: 'circle-slash',
      FAILING: 'warning',
      CANCELLING: 'loading~spin',
      CREATED: 'circle-outline',
      RESTARTING: 'sync~spin',
      SUSPENDED: 'debug-pause',
      RECONCILING: 'sync~spin',
    };

    return new vscode.ThemeIcon(iconMap[state] || 'circle-outline');
  }

  private getJobDescription(job: JobSummary): string {
    const parts: string[] = [];

    // Show task progress for running jobs
    if (job.state === 'RUNNING' && job.tasks) {
      parts.push(`${job.tasks.running}/${job.tasks.total} tasks`);
    }

    // Show completion time for finished jobs
    if (job.state === 'FINISHED' && job['end-time']) {
      const endTime = new Date(job['end-time']);
      const now = new Date();
      const diffMs = now.getTime() - endTime.getTime();
      const diffMins = Math.floor(diffMs / 60000);

      if (diffMins < 60) {
        parts.push(`${diffMins}m ago`);
      } else {
        const diffHours = Math.floor(diffMins / 60);
        parts.push(`${diffHours}h ago`);
      }
    }

    return parts.join(' • ');
  }

  private getJobTooltip(job: JobSummary): string {
    const lines: string[] = [];

    lines.push(`Job: ${job.name}`);
    lines.push(`ID: ${job.jid}`);
    lines.push(`State: ${job.state}`);

    if (job['start-time']) {
      lines.push(`Started: ${new Date(job['start-time']).toLocaleString()}`);
    }

    if (job['end-time']) {
      lines.push(`Ended: ${new Date(job['end-time']).toLocaleString()}`);
    }

    lines.push(`Duration: ${this.formatDuration(job.duration)}`);

    if (job.tasks) {
      lines.push('');
      lines.push('Tasks:');
      lines.push(`  Total: ${job.tasks.total}`);
      lines.push(`  Running: ${job.tasks.running}`);
      lines.push(`  Finished: ${job.tasks.finished}`);
      if (job.tasks.failed > 0) {
        lines.push(`  Failed: ${job.tasks.failed}`);
      }
    }

    return lines.join('\n');
  }

  private formatDuration(durationMs: number): string {
    if (durationMs < 0) return 'N/A';

    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
      return `${days}d ${hours % 24}h`;
    } else if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  private createEmptyState(message: string): JobTreeItem[] {
    const item = new JobTreeItem(
      message,
      'category',
      vscode.TreeItemCollapsibleState.None
    );

    item.iconPath = new vscode.ThemeIcon('info');
    item.tooltip = message;

    return [item];
  }

  /**
   * Get formatted job details for output channel
   */
  async getJobDetailsText(jobId: string): Promise<string> {
    try {
      const details = await this.jobClient.getJobDetails(jobId);

      const lines: string[] = [];
      lines.push(`Job Details: ${details.name}`);
      lines.push('─'.repeat(50));
      lines.push(`Job ID: ${details.jid}`);
      lines.push(`State: ${details.state}`);
      lines.push(`Started: ${new Date(details['start-time']).toLocaleString()}`);

      if (details['end-time'] > 0) {
        lines.push(`Ended: ${new Date(details['end-time']).toLocaleString()}`);
      }

      lines.push(`Duration: ${this.formatDuration(details.duration)}`);
      lines.push('');

      // Task summary
      if (details['status-counts']) {
        lines.push('Task Status:');
        for (const [status, count] of Object.entries(details['status-counts'])) {
          lines.push(`  ${status}: ${count}`);
        }
        lines.push('');
      }

      // Vertices (operators)
      if (details.vertices && details.vertices.length > 0) {
        lines.push('Operators:');
        for (const vertex of details.vertices) {
          lines.push(`  - ${vertex.name} (parallelism: ${vertex.parallelism})`);
          lines.push(`    Status: ${vertex.status}`);
        }
      }

      return lines.join('\n');
    } catch (error) {
      return `Error fetching job details: ${error instanceof Error ? error.message : String(error)}`;
    }
  }

  /**
   * Get formatted job metrics for output channel
   */
  async getJobMetricsText(jobId: string): Promise<string> {
    try {
      // Get common metrics
      const metricNames = [
        'numRecordsIn',
        'numRecordsOut',
        'numBytesIn',
        'numBytesOut',
      ];

      const metricsResponse = await this.jobClient.getJobMetrics(jobId, metricNames);
      const checkpoints = await this.jobClient.getCheckpoints(jobId);

      const lines: string[] = [];
      lines.push(`Job Metrics: ${jobId}`);
      lines.push('─'.repeat(50));

      if (metricsResponse.metrics && metricsResponse.metrics.length > 0) {
        lines.push('Records & Bytes:');
        for (const metric of metricsResponse.metrics) {
          lines.push(`  ${metric.id}: ${metric.value}`);
        }
        lines.push('');
      }

      if (checkpoints) {
        lines.push('Checkpoints:');
        lines.push(`  Latest: ${checkpoints.latest?.completed ? 'Completed' : 'None'}`);
        if (checkpoints.counts) {
          lines.push(`  Total: ${checkpoints.counts.total || 0}`);
          lines.push(`  Completed: ${checkpoints.counts.completed || 0}`);
          lines.push(`  Failed: ${checkpoints.counts.failed || 0}`);
        }
      }

      return lines.join('\n');
    } catch (error) {
      return `Error fetching job metrics: ${error instanceof Error ? error.message : String(error)}`;
    }
  }

  dispose(): void {
    this.stopAutoRefresh();
  }
}

export class JobTreeItem extends vscode.TreeItem {
  constructor(
    public readonly label: string,
    public readonly itemType: JobTreeItemType,
    public readonly collapsibleState: vscode.TreeItemCollapsibleState,
    public readonly job?: JobSummary,
    public readonly state?: JobState,
    public readonly jobs?: JobSummary[]
  ) {
    super(label, collapsibleState);

    if (itemType === 'category') {
      this.contextValue = 'category';
    }
  }
}
