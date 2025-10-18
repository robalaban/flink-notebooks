/**
 * Type definitions for Flink Notebooks
 */

export interface ClusterStatus {
  status: 'stopped' | 'starting' | 'running' | 'stopping' | 'error';
  pid?: number;
  gateway_url?: string;
}

export interface Session {
  session_id: string;
  session_name?: string;
  created_at: string;
  properties: Record<string, string>;
}

export interface ExecuteResponse {
  statement_id: string;
  status: 'pending' | 'running' | 'completed' | 'canceled' | 'failed';
  job_id?: string;
}

export interface ResultRow {
  kind: string;
  fields: unknown[];
}

export interface StreamMessage {
  type: 'row' | 'status' | 'error' | 'complete';
  data: unknown;
  statement_id?: string;
}

export interface CatalogNode {
  type: 'catalog' | 'database' | 'table';
  name: string;
  children: CatalogNode[];
  metadata: Record<string, string>;
}

export interface Table {
  name: string;
  database: string;
  catalog?: string;
  table_type?: string;
  location?: string;
  schema: TableColumn[];
}

export interface TableColumn {
  column_name: string;
  data_type: string;
  nullable?: boolean;
  comment?: string;
}

export interface NotebookMetadata {
  session_id?: string;
  cluster_status?: string;
  aws_profile?: string;
}

export interface CellMetadata {
  execution_count?: number;
  execution_time_ms?: number;
  statement_id?: string;
  job_id?: string;
  is_streaming?: boolean;
  streaming_started?: number;
  total_rows_fetched?: number;
}
