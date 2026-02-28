import { DatabaseManager } from "../database.js";

export interface TableColumn {
  column_name: string;
  data_type: string;
  is_nullable: string;
  column_default: string | null;
  character_maximum_length: number | null;
  numeric_precision: number | null;
}

export interface ForeignKey {
  constraint_name: string;
  column_name: string;
  foreign_table_schema: string;
  foreign_table_name: string;
  foreign_column_name: string;
}

export interface IndexInfo {
  index_name: string;
  index_definition: string;
  is_unique: boolean;
  is_primary: boolean;
  columns: string;
}

export interface TableStats {
  table_name: string;
  schema_name: string;
  estimated_row_count: number;
  total_size: string;
  table_size: string;
  index_size: string;
  last_vacuum: string | null;
  last_analyze: string | null;
  live_tuples: number;
  dead_tuples: number;
}

/**
 * Tools for exploring and introspecting database schemas.
 */
export class SchemaTools {
  constructor(private db: DatabaseManager) {}

  /**
   * List all schemas in the current database.
   */
  async listSchemas(): Promise<string[]> {
    const result = await this.db.queryReadOnly<{ schema_name: string }>(
      `SELECT schema_name 
       FROM information_schema.schemata 
       WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
       ORDER BY schema_name`
    );
    return result.rows.map((r) => r.schema_name);
  }

  /**
   * List all tables in a given schema.
   */
  async listTables(schema: string = "public"): Promise<string[]> {
    const result = await this.db.queryReadOnly<{ table_name: string }>(
      `SELECT table_name 
       FROM information_schema.tables 
       WHERE table_schema = $1 AND table_type = 'BASE TABLE'
       ORDER BY table_name`,
      [schema]
    );
    return result.rows.map((r) => r.table_name);
  }

  /**
   * List all views in a given schema.
   */
  async listViews(schema: string = "public"): Promise<string[]> {
    const result = await this.db.queryReadOnly<{ table_name: string }>(
      `SELECT table_name 
       FROM information_schema.views 
       WHERE table_schema = $1
       ORDER BY table_name`,
      [schema]
    );
    return result.rows.map((r) => r.table_name);
  }

  /**
   * Describe a table's columns, types, and constraints.
   */
  async describeTable(
    table: string,
    schema: string = "public"
  ): Promise<TableColumn[]> {
    const result = await this.db.queryReadOnly<TableColumn>(
      `SELECT 
        column_name, 
        data_type, 
        is_nullable, 
        column_default,
        character_maximum_length,
        numeric_precision
       FROM information_schema.columns 
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schema, table]
    );
    return result.rows;
  }

  /**
   * Get foreign key relationships for a table.
   */
  async getForeignKeys(
    table: string,
    schema: string = "public"
  ): Promise<ForeignKey[]> {
    const result = await this.db.queryReadOnly<ForeignKey>(
      `SELECT
        tc.constraint_name,
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
       FROM information_schema.table_constraints AS tc
       JOIN information_schema.key_column_usage AS kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       JOIN information_schema.constraint_column_usage AS ccu
         ON ccu.constraint_name = tc.constraint_name
         AND ccu.table_schema = tc.table_schema
       WHERE tc.constraint_type = 'FOREIGN KEY'
         AND tc.table_schema = $1
         AND tc.table_name = $2
       ORDER BY tc.constraint_name`,
      [schema, table]
    );
    return result.rows;
  }

  /**
   * List indexes on a table.
   */
  async listIndexes(
    table: string,
    schema: string = "public"
  ): Promise<IndexInfo[]> {
    const result = await this.db.queryReadOnly<IndexInfo>(
      `SELECT
        i.relname AS index_name,
        pg_get_indexdef(ix.indexrelid) AS index_definition,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ', ') AS columns
       FROM pg_class t
       JOIN pg_index ix ON t.oid = ix.indrelid
       JOIN pg_class i ON i.oid = ix.indexrelid
       JOIN pg_namespace n ON n.oid = t.relnamespace
       JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
       WHERE n.nspname = $1 AND t.relname = $2
       GROUP BY i.relname, ix.indexrelid, ix.indisunique, ix.indisprimary
       ORDER BY i.relname`,
      [schema, table]
    );
    return result.rows;
  }

  /**
   * Get table statistics (size, row counts, vacuum info).
   */
  async getTableStats(
    table: string,
    schema: string = "public"
  ): Promise<TableStats | null> {
    const result = await this.db.queryReadOnly<TableStats>(
      `SELECT
        c.relname AS table_name,
        n.nspname AS schema_name,
        c.reltuples::bigint AS estimated_row_count,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
        pg_size_pretty(pg_table_size(c.oid)) AS table_size,
        pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
        s.last_vacuum::text AS last_vacuum,
        s.last_analyze::text AS last_analyze,
        s.n_live_tup AS live_tuples,
        s.n_dead_tup AS dead_tuples
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
       WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
      [schema, table]
    );
    return result.rows[0] ?? null;
  }

  /**
   * Get all primary key and unique constraints for a table.
   */
  async getConstraints(
    table: string,
    schema: string = "public"
  ): Promise<
    { constraint_name: string; constraint_type: string; columns: string }[]
  > {
    const result = await this.db.queryReadOnly<{
      constraint_name: string;
      constraint_type: string;
      columns: string;
    }>(
      `SELECT
        tc.constraint_name,
        tc.constraint_type,
        string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.table_schema = $1
         AND tc.table_name = $2
         AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK')
       GROUP BY tc.constraint_name, tc.constraint_type
       ORDER BY tc.constraint_type, tc.constraint_name`,
      [schema, table]
    );
    return result.rows;
  }
}
