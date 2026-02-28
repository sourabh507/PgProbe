import { Pool, PoolConfig, QueryResult } from "pg";

export interface ConnectionConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean;
}

/**
 * Manages PostgreSQL connection pooling and query execution.
 * Enforces read-only mode by default for safety.
 */
export class DatabaseManager {
  private pool: Pool | null = null;
  private config: ConnectionConfig | null = null;

  /**
   * Connect to a PostgreSQL database.
   */
  async connect(config: ConnectionConfig): Promise<void> {
    if (this.pool) {
      await this.disconnect();
    }

    const poolConfig: PoolConfig = {
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    };

    this.pool = new Pool(poolConfig);
    this.config = config;

    // Test the connection
    const client = await this.pool.connect();
    client.release();
  }

  /**
   * Disconnect from the database.
   */
  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.pool = null;
      this.config = null;
    }
  }

  /**
   * Check if currently connected to a database.
   */
  isConnected(): boolean {
    return this.pool !== null;
  }

  /**
   * Get current connection info.
   */
  getConnectionInfo(): ConnectionConfig | null {
    return this.config;
  }

  /**
   * Execute a read-only query. Wraps in a read-only transaction for safety.
   */
  async queryReadOnly<T extends object = Record<string, unknown>>(
    sql: string,
    params?: unknown[]
  ): Promise<QueryResult<T>> {
    this.ensureConnected();

    const client = await this.pool!.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      const result = await client.query<T>(sql, params);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Execute a query (for write operations – use with caution).
   */
  async query<T extends object = Record<string, unknown>>(
    sql: string,
    params?: unknown[]
  ): Promise<QueryResult<T>> {
    this.ensureConnected();
    return this.pool!.query<T>(sql, params);
  }

  /**
   * Ensure the pool is connected. Throws if not.
   */
  private ensureConnected(): void {
    if (!this.pool) {
      throw new Error(
        "Not connected to any database. Use the 'connect' tool first."
      );
    }
  }
}
