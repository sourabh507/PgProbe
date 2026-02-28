import { DatabaseManager } from "../database.js";

export interface IndexSuggestion {
  table: string;
  columns: string[];
  reason: string;
  impact: "high" | "medium" | "low";
  createStatement: string;
}

export interface SlowQuery {
  query: string;
  calls: number;
  totalTimeMs: number;
  meanTimeMs: number;
  stddevTimeMs: number;
  rows: number;
}

export interface TableBloat {
  table_name: string;
  schema_name: string;
  bloat_ratio: number;
  wasted_bytes: string;
  table_size: string;
}

/**
 * Tools for analyzing and optimizing database performance.
 */
export class OptimizationTools {
  constructor(private db: DatabaseManager) {}

  /**
   * Analyze a query's execution plan and suggest indexes.
   */
  async suggestIndexes(
    sql: string,
    schema: string = "public"
  ): Promise<IndexSuggestion[]> {
    const suggestions: IndexSuggestion[] = [];

    // Get the query plan
    const result = await this.db.queryReadOnly<{ "QUERY PLAN": unknown[] }>(
      `EXPLAIN (FORMAT JSON) ${sql}`
    );

    const planJson = result.rows[0]?.["QUERY PLAN"];
    const plan = (Array.isArray(planJson) ? planJson[0] : planJson) as Record<
      string,
      unknown
    >;

    // Walk the plan tree and find opportunities
    this.analyzePlanNode(
      (plan["Plan"] ?? plan) as Record<string, unknown>,
      schema,
      suggestions
    );

    return suggestions;
  }

  /**
   * Find the slowest queries using pg_stat_statements (if available).
   */
  async getSlowQueries(limit: number = 10): Promise<SlowQuery[]> {
    try {
      const result = await this.db.queryReadOnly<SlowQuery>(
        `SELECT
          query,
          calls,
          round(total_exec_time::numeric, 2) AS "totalTimeMs",
          round(mean_exec_time::numeric, 2) AS "meanTimeMs",
          round(stddev_exec_time::numeric, 2) AS "stddevTimeMs",
          rows
         FROM pg_stat_statements
         WHERE query NOT LIKE '%pg_stat_statements%'
         ORDER BY mean_exec_time DESC
         LIMIT $1`,
        [limit]
      );
      return result.rows;
    } catch {
      // pg_stat_statements extension may not be installed
      throw new Error(
        "pg_stat_statements extension is not available. " +
          "Install it with: CREATE EXTENSION pg_stat_statements;"
      );
    }
  }

  /**
   * Find unused indexes (indexes that have never been scanned).
   */
  async findUnusedIndexes(
    schema: string = "public"
  ): Promise<
    {
      index_name: string;
      table_name: string;
      index_size: string;
      index_scans: number;
    }[]
  > {
    const result = await this.db.queryReadOnly<{
      index_name: string;
      table_name: string;
      index_size: string;
      index_scans: number;
    }>(
      `SELECT
        s.indexrelname AS index_name,
        s.relname AS table_name,
        pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
        s.idx_scan AS index_scans
       FROM pg_stat_user_indexes s
       JOIN pg_index i ON s.indexrelid = i.indexrelid
       WHERE s.schemaname = $1
         AND s.idx_scan = 0
         AND NOT i.indisunique
         AND NOT i.indisprimary
       ORDER BY pg_relation_size(s.indexrelid) DESC`,
      [schema]
    );
    return result.rows;
  }

  /**
   * Find duplicate indexes (indexes covering the same columns).
   */
  async findDuplicateIndexes(
    schema: string = "public"
  ): Promise<
    {
      table_name: string;
      index1: string;
      index2: string;
      columns: string;
    }[]
  > {
    const result = await this.db.queryReadOnly<{
      table_name: string;
      index1: string;
      index2: string;
      columns: string;
    }>(
      `WITH index_info AS (
        SELECT
          n.nspname AS schema_name,
          t.relname AS table_name,
          i.relname AS index_name,
          array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ', ') AS columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = $1
        GROUP BY n.nspname, t.relname, i.relname
      )
      SELECT
        a.table_name,
        a.index_name AS index1,
        b.index_name AS index2,
        a.columns
      FROM index_info a
      JOIN index_info b ON a.table_name = b.table_name
        AND a.columns = b.columns
        AND a.index_name < b.index_name
      ORDER BY a.table_name`,
      [schema]
    );
    return result.rows;
  }

  /**
   * Estimate table bloat (wasted space from dead tuples).
   */
  async getTableBloat(schema: string = "public"): Promise<TableBloat[]> {
    const result = await this.db.queryReadOnly<TableBloat>(
      `SELECT
        relname AS table_name,
        schemaname AS schema_name,
        CASE WHEN n_live_tup > 0
          THEN round((n_dead_tup::numeric / n_live_tup) * 100, 2)
          ELSE 0
        END AS bloat_ratio,
        pg_size_pretty(
          pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname))
          - pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname))
        ) AS wasted_bytes,
        pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname))) AS table_size
       FROM pg_stat_user_tables
       WHERE schemaname = $1
       ORDER BY n_dead_tup DESC
       LIMIT 20`,
      [schema]
    );
    return result.rows;
  }

  /**
   * Get overall database health metrics.
   */
  async getDatabaseHealth(): Promise<Record<string, unknown>> {
    const [sizeResult, connectionsResult, cacheResult, txResult] =
      await Promise.all([
        this.db.queryReadOnly<{ db_size: string }>(
          `SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size`
        ),
        this.db.queryReadOnly<{
          active: string;
          idle: string;
          max: string;
        }>(
          `SELECT
            count(*) FILTER (WHERE state = 'active')::text AS active,
            count(*) FILTER (WHERE state = 'idle')::text AS idle,
            (SELECT setting FROM pg_settings WHERE name = 'max_connections') AS max
           FROM pg_stat_activity`
        ),
        this.db.queryReadOnly<{ cache_hit_ratio: string }>(
          `SELECT
            round(
              sum(heap_blks_hit)::numeric /
              NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100, 2
            )::text AS cache_hit_ratio
           FROM pg_statio_user_tables`
        ),
        this.db.queryReadOnly<{
          total_commits: string;
          total_rollbacks: string;
        }>(
          `SELECT
            xact_commit::text AS total_commits,
            xact_rollback::text AS total_rollbacks
           FROM pg_stat_database
           WHERE datname = current_database()`
        ),
      ]);

    return {
      database_size: sizeResult.rows[0]?.db_size,
      connections: connectionsResult.rows[0],
      cache_hit_ratio: cacheResult.rows[0]?.cache_hit_ratio
        ? `${cacheResult.rows[0].cache_hit_ratio}%`
        : "N/A",
      transactions: txResult.rows[0],
    };
  }

  /**
   * Recursively analyze plan nodes for index suggestions.
   */
  private analyzePlanNode(
    node: Record<string, unknown>,
    schema: string,
    suggestions: IndexSuggestion[]
  ): void {
    if (!node) return;

    const nodeType = node["Node Type"] as string;
    const table = node["Relation Name"] as string;
    const filter = node["Filter"] as string;
    const estimatedRows = (node["Plan Rows"] as number) ?? 0;

    // Sequential scan with a filter → suggest index on filtered columns
    if (nodeType === "Seq Scan" && filter && estimatedRows > 1000) {
      const columns = this.extractColumnsFromFilter(filter);
      if (columns.length > 0) {
        const impact =
          estimatedRows > 100000
            ? "high"
            : estimatedRows > 10000
              ? "medium"
              : "low";
        suggestions.push({
          table,
          columns,
          reason: `Sequential scan with filter on ${columns.join(", ")} scanning ~${estimatedRows} rows`,
          impact,
          createStatement: `CREATE INDEX idx_${table}_${columns.join("_")} ON ${schema}.${table} (${columns.join(", ")});`,
        });
      }
    }

    // Sort without an index
    if (nodeType === "Sort") {
      const sortKey = node["Sort Key"] as string[];
      if (sortKey && Array.isArray(sortKey)) {
        const columns = sortKey.map((k) =>
          k.replace(/.*\./, "").replace(/ (ASC|DESC)/i, "")
        );
        if (columns.length > 0 && estimatedRows > 5000) {
          suggestions.push({
            table: table ?? "unknown",
            columns,
            reason: `Sort operation on ${columns.join(", ")} with ~${estimatedRows} rows. An index could eliminate the sort.`,
            impact: estimatedRows > 50000 ? "high" : "medium",
            createStatement: `-- Consider adding an index to support this sort:\n-- CREATE INDEX idx_sort_${columns.join("_")} ON ${schema}.<table> (${columns.join(", ")});`,
          });
        }
      }
    }

    // Recurse into child plans
    const plans = node["Plans"] as Record<string, unknown>[];
    if (Array.isArray(plans)) {
      for (const child of plans) {
        this.analyzePlanNode(child, schema, suggestions);
      }
    }
  }

  /**
   * Extract column names from a PostgreSQL filter expression.
   */
  private extractColumnsFromFilter(filter: string): string[] {
    const columns: string[] = [];
    // Match patterns like (column_name = ...) or (column_name > ...)
    const regex = /\((\w+)\s*[=<>!]+/g;
    let match;
    while ((match = regex.exec(filter)) !== null) {
      const col = match[1];
      if (col && !columns.includes(col)) {
        columns.push(col);
      }
    }
    return columns;
  }
}
