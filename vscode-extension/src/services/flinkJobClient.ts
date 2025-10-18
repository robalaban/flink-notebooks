/**
 * Client for communicating with Flink JobManager REST API
 */

import axios, { AxiosInstance } from 'axios';

export type JobState =
  | 'CREATED'
  | 'RUNNING'
  | 'FAILING'
  | 'FAILED'
  | 'CANCELLING'
  | 'CANCELED'
  | 'FINISHED'
  | 'RESTARTING'
  | 'SUSPENDED'
  | 'RECONCILING';

export interface JobSummary {
  jid: string;
  name: string;
  state: JobState;
  'start-time': number;
  'end-time': number;
  duration: number;
  'last-modification': number;
  tasks: {
    total: number;
    created: number;
    scheduled: number;
    deploying: number;
    running: number;
    finished: number;
    canceling: number;
    canceled: number;
    failed: number;
  };
}

export interface JobOverview {
  jobs: JobSummary[];
}

export interface JobVertex {
  id: string;
  name: string;
  parallelism: number;
  status: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  tasks: Record<string, number>;
  metrics: Record<string, number>;
}

export interface JobDetails {
  jid: string;
  name: string;
  isStoppable: boolean;
  state: JobState;
  'start-time': number;
  'end-time': number;
  duration: number;
  now: number;
  timestamps: Record<string, number>;
  vertices: JobVertex[];
  'status-counts': Record<string, number>;
  plan?: any;
}

export interface JobMetric {
  id: string;
  value: string;
}

export interface JobMetricsResponse {
  metrics: JobMetric[];
}

export class FlinkJobClient {
  private client: AxiosInstance;
  private baseUrl: string;

  constructor(baseUrl: string = 'http://localhost:8081') {
    this.baseUrl = baseUrl;
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 10000,
    });
  }

  /**
   * Check if Flink JobManager REST API is available
   */
  async isAvailable(): Promise<boolean> {
    try {
      await this.client.get('/config');
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get overview of all jobs (running, finished, failed, etc.)
   */
  async getJobOverview(): Promise<JobOverview> {
    try {
      const response = await this.client.get('/jobs/overview');
      return response.data;
    } catch (error) {
      console.error('Error fetching job overview:', error);
      throw new Error('Failed to fetch job overview from Flink REST API');
    }
  }

  /**
   * Get detailed information about a specific job
   */
  async getJobDetails(jobId: string): Promise<JobDetails> {
    try {
      const response = await this.client.get(`/jobs/${jobId}`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching job details for ${jobId}:`, error);
      throw new Error(`Failed to fetch details for job ${jobId}`);
    }
  }

  /**
   * Get metrics for a specific job
   */
  async getJobMetrics(jobId: string, metricIds?: string[]): Promise<JobMetricsResponse> {
    try {
      const params: any = {};
      if (metricIds && metricIds.length > 0) {
        params.get = metricIds.join(',');
      }

      const response = await this.client.get(`/jobs/${jobId}/metrics`, { params });
      return { metrics: response.data };
    } catch (error) {
      console.error(`Error fetching metrics for job ${jobId}:`, error);
      throw new Error(`Failed to fetch metrics for job ${jobId}`);
    }
  }

  /**
   * Get available metric names for a job
   */
  async getAvailableMetrics(jobId: string): Promise<string[]> {
    try {
      const response = await this.client.get(`/jobs/${jobId}/metrics`);
      return response.data.map((metric: any) => metric.id);
    } catch (error) {
      console.error(`Error fetching available metrics for job ${jobId}:`, error);
      return [];
    }
  }

  /**
   * Cancel a running job
   */
  async cancelJob(jobId: string): Promise<void> {
    try {
      // Flink REST API expects PATCH /jobs/:jobid with query param mode=cancel
      await this.client.patch(`/jobs/${jobId}?mode=cancel`);
    } catch (error: any) {
      console.error(`Error canceling job ${jobId}:`, error);

      // Extract more detailed error message
      const errorMsg = error.response?.data?.errors?.[0] || error.message || String(error);
      throw new Error(`Failed to cancel job ${jobId}: ${errorMsg}`);
    }
  }

  /**
   * Get checkpoints information for a job
   */
  async getCheckpoints(jobId: string): Promise<any> {
    try {
      const response = await this.client.get(`/jobs/${jobId}/checkpoints`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching checkpoints for job ${jobId}:`, error);
      return null;
    }
  }

  /**
   * Get Flink configuration
   */
  async getConfig(): Promise<any> {
    try {
      const response = await this.client.get('/config');
      return response.data;
    } catch (error) {
      console.error('Error fetching Flink config:', error);
      return null;
    }
  }

  /**
   * Build Web UI URL for a specific job
   */
  getJobWebUIUrl(jobId: string): string {
    // Flink Web UI uses hash routing for jobs
    return `${this.baseUrl}/#/job/running/${jobId}/overview`;
  }

  /**
   * Build Web UI URL for the overview page
   */
  getWebUIUrl(): string {
    // Point to the jobs overview page
    return `${this.baseUrl}/#/overview`;
  }
}
