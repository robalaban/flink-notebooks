/**
 * Flink Catalog integration service
 * Queries Flink SQL Gateway to list catalogs, databases, and tables
 */

import { SqlGatewayClient } from './sqlGatewayClient';

export interface Database {
  name: string;
  catalog: string;
  description?: string;
}

export interface TableSchema {
  column_name: string;
  data_type: string;
  comment?: string;
}

export interface Table {
  name: string;
  database: string;
  catalog: string;
  table_type?: string;
  schema?: TableSchema[];
}

export interface CatalogNode {
  type: 'catalog' | 'database' | 'table';
  name: string;
  metadata?: Record<string, any>;
  children?: CatalogNode[];
}

export class CatalogService {
  private sessionId: string | null = null;

  constructor(
    private gatewayClient: SqlGatewayClient,
    private getSessionId: () => Promise<string>
  ) {}

  /**
   * Check if Flink SQL Gateway is available
   */
  async isAvailable(): Promise<boolean> {
    try {
      return await this.gatewayClient.isAvailable();
    } catch {
      return false;
    }
  }

  /**
   * Execute a SQL statement and return the results as an array of row objects
   */
  private async executeSqlQuery(sql: string): Promise<any[]> {
    const sessionId = await this.getSessionId();

    const result = await this.gatewayClient.executeStatement(sessionId, sql);
    const operationHandle = result.operationHandle;

    // Wait for operation to complete
    let statusInfo;
    let attempts = 0;
    const maxAttempts = 30;

    while (attempts < maxAttempts) {
      statusInfo = await this.gatewayClient.getStatementInfo(sessionId, operationHandle);

      if (statusInfo.status === 'ERROR') {
        throw new Error('Query execution failed');
      }

      if (statusInfo.status === 'FINISHED' || statusInfo.status === 'RUNNING') {
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, 500));
      attempts++;
    }

    // Fetch results
    const rows: any[] = [];
    let token = 0;
    let isComplete = false;

    while (!isComplete) {
      const fetchResult = await this.gatewayClient.fetchResults(sessionId, operationHandle, token, 100);
      const rawData = fetchResult.results?.data || fetchResult.data || [];
      const columns = fetchResult.results?.columns || [];

      // Transform field-based rows to objects
      if (rawData.length > 0 && columns.length > 0) {
        const transformedRows = rawData.map((row: any) => {
          const fields = row.fields || [];
          const obj: Record<string, any> = {};

          columns.forEach((col: any, index: number) => {
            obj[col.name] = fields[index];
          });

          return obj;
        });

        rows.push(...transformedRows);
      }

      // Check for more results
      const nextToken = fetchResult.nextResultUri;
      isComplete = fetchResult.resultType === 'EOS' || !nextToken;

      if (!isComplete && nextToken && nextToken.includes('/')) {
        token = parseInt(nextToken.split('/').pop() || '0');
      } else {
        token += 1;
      }
    }

    return rows;
  }

  /**
   * List all catalogs visible to Flink
   */
  async listCatalogs(): Promise<string[]> {
    try {
      const rows = await this.executeSqlQuery('SHOW CATALOGS');

      // Results format: [{ catalog_name: 'default_catalog' }, ...]
      return rows.map(row => row['catalog name'] || row.catalog_name || row.name || Object.values(row)[0]);
    } catch (error) {
      console.error('Error listing catalogs:', error);
      throw new Error(
        'Failed to list catalogs. Make sure the Flink cluster is running.\n\n' +
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * List databases in a specific catalog
   */
  async listDatabases(catalog: string): Promise<Database[]> {
    try {
      // Use the catalog
      await this.executeSqlQuery(`USE CATALOG \`${catalog}\``);

      // Show databases
      const rows = await this.executeSqlQuery('SHOW DATABASES');

      // Results format: [{ database_name: 'default_database' }, ...]
      return rows.map(row => ({
        name: row['database name'] || row.database_name || row.name || Object.values(row)[0],
        catalog: catalog,
      }));
    } catch (error) {
      console.error(`Error listing databases in catalog ${catalog}:`, error);
      throw new Error(
        `Failed to list databases in catalog '${catalog}'.\n\n` +
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * List tables in a specific database
   */
  async listTables(catalog: string, database: string): Promise<string[]> {
    try {
      // Use the catalog and database
      await this.executeSqlQuery(`USE CATALOG \`${catalog}\``);
      await this.executeSqlQuery(`USE \`${database}\``);

      // Show tables
      const rows = await this.executeSqlQuery('SHOW TABLES');

      // Results format: [{ table_name: 'my_table' }, ...]
      return rows.map(row => row['table name'] || row.table_name || row.name || Object.values(row)[0]);
    } catch (error) {
      console.error(`Error listing tables in ${catalog}.${database}:`, error);
      throw new Error(
        `Failed to list tables in '${catalog}.${database}'.\n\n` +
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Get detailed table information
   */
  async getTable(catalog: string, database: string, table: string): Promise<Table> {
    try {
      // Use the catalog and database
      await this.executeSqlQuery(`USE CATALOG \`${catalog}\``);
      await this.executeSqlQuery(`USE \`${database}\``);

      // Describe the table
      const rows = await this.executeSqlQuery(`DESCRIBE \`${table}\``);

      // Parse schema from DESCRIBE output
      const schema: TableSchema[] = rows.map(row => ({
        column_name: row.name || row.column_name || row['column name'] || '',
        data_type: row.type || row.data_type || row['data type'] || '',
        comment: row.comment || row.description,
      }));

      return {
        name: table,
        database,
        catalog,
        schema,
      };
    } catch (error) {
      console.error(`Error getting table ${catalog}.${database}.${table}:`, error);
      throw new Error(
        `Failed to get table details for '${catalog}.${database}.${table}'.\n\n` +
        `Error: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Build the complete catalog tree hierarchy
   */
  async getCatalogTree(): Promise<CatalogNode[]> {
    try {
      const catalogNames = await this.listCatalogs();
      const catalogNodes: CatalogNode[] = [];

      for (const catalogName of catalogNames) {
        const catalogNode: CatalogNode = {
          type: 'catalog',
          name: catalogName,
          children: [],
        };

        try {
          const databases = await this.listDatabases(catalogName);

          for (const db of databases) {
            const dbNode: CatalogNode = {
              type: 'database',
              name: db.name,
              metadata: {
                catalog: catalogName,
              },
              children: [],
            };

            try {
              const tables = await this.listTables(catalogName, db.name);

              for (const tableName of tables) {
                const tableNode: CatalogNode = {
                  type: 'table',
                  name: tableName,
                  metadata: {
                    catalog: catalogName,
                    database: db.name,
                  },
                };
                dbNode.children!.push(tableNode);
              }
            } catch (error) {
              console.warn(`Failed to list tables in ${catalogName}.${db.name}:`, error);
              // Continue with other databases
            }

            catalogNode.children!.push(dbNode);
          }
        } catch (error) {
          console.warn(`Failed to list databases in catalog ${catalogName}:`, error);
          // Continue with other catalogs
        }

        catalogNodes.push(catalogNode);
      }

      return catalogNodes;
    } catch (error) {
      console.error('Error building catalog tree:', error);
      throw error;
    }
  }
}
