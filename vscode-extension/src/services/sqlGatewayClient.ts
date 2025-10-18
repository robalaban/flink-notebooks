/**
 * Client for communicating with Flink SQL Gateway REST API directly
 */

import axios, { AxiosInstance } from 'axios';

export interface SessionInfo {
  sessionHandle: string;
}

export interface StatementResult {
  operationHandle: string;
}

export interface OperationStatus {
  status: string;
}

export interface ColumnInfo {
  name: string;
  logicalType: {
    type: string;
    nullable: boolean;
    [key: string]: any;
  };
  comment?: string;
}

export interface ResultSet {
  resultType?: string;
  isQueryResult?: boolean;
  jobID?: string;
  resultKind?: string;
  results?: {
    columns?: ColumnInfo[];
    rowFormat?: string;
    data?: any[];
  };
  nextResultUri?: string;
  data?: any[]; // Legacy support
}

export class SqlGatewayClient {
  private client: AxiosInstance;
  private baseUrl: string;

  constructor(baseUrl: string = 'http://localhost:8083') {
    this.baseUrl = baseUrl;
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 30000,
    });
  }

  // Gateway info
  async getInfo(): Promise<any> {
    const response = await this.client.get('/v1/info');
    return response.data;
  }

  async isAvailable(): Promise<boolean> {
    try {
      await this.getInfo();
      return true;
    } catch {
      return false;
    }
  }

  // Session management
  async createSession(
    sessionName?: string,
    properties?: Record<string, string>
  ): Promise<SessionInfo> {
    const payload: any = {};
    if (sessionName) {
      payload.sessionName = sessionName;
    }
    if (properties) {
      payload.properties = properties;
    }

    const response = await this.client.post('/v1/sessions', payload);
    return response.data;
  }

  async getSession(sessionHandle: string): Promise<any> {
    const response = await this.client.get(`/v1/sessions/${sessionHandle}`);
    return response.data;
  }

  async closeSession(sessionHandle: string): Promise<void> {
    await this.client.delete(`/v1/sessions/${sessionHandle}`);
  }

  // Statement execution
  async executeStatement(
    sessionHandle: string,
    statement: string,
    executionTimeout?: number
  ): Promise<StatementResult> {
    const payload: any = { statement };
    if (executionTimeout !== undefined) {
      payload.executionTimeout = executionTimeout;
    }

    const response = await this.client.post(
      `/v1/sessions/${sessionHandle}/statements`,
      payload
    );
    return response.data;
  }

  async getStatementInfo(
    sessionHandle: string,
    operationHandle: string
  ): Promise<OperationStatus> {
    const response = await this.client.get(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}/status`
    );
    return response.data;
  }

  async fetchResults(
    sessionHandle: string,
    operationHandle: string,
    token: number = 0,
    maxRows: number = 100
  ): Promise<ResultSet> {
    const response = await this.client.get(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}/result/${token}`,
      {
        params: {
          rowFormat: 'JSON',
          maxRows,
        },
      }
    );
    return response.data;
  }

  async cancelOperation(
    sessionHandle: string,
    operationHandle: string
  ): Promise<void> {
    await this.client.delete(
      `/v1/sessions/${sessionHandle}/operations/${operationHandle}`
    );
  }
}
