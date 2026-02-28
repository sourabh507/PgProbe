import { DatabaseManager } from "../database.js";

export interface QueryResultFormatted {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
}

export interface QueryPlan {
  plan: string;
  planningTimeMs: number;
  executionTimeMs: number;
  warnings: string[];
}

/**
 * Tools for executing and analyzing SQL queries.
 */
export class QueryTools {
  constructor(private db: DatabaseManager) {}

  /**
   * Execute a read-only SQL query and return formatted results.
   */
  async executeQuery(
    sql: string,
    params?: unknown[],
    limit: number = 100
  ): Promise<QueryResultFormatted> {
    // Validate: block destructive operations
    this.validateReadOnly(sql);

    // Add LIMIT if not present and it's a SELECT
    const normalizedSql = this.ensureLimit(sql, limit);

    const start = performance.now();
    const result = await this.db.queryReadOnly(normalizedSql, params);
    const executionTimeMs = Math.round(performance.now() - start);

    return {
      columns: result.fields.map((f) => f.name),
      rows: result.rows,
      rowCount: result.rowCount ?? 0,
      executionTimeMs,
    };
  }

  /**
   * Run EXPLAIN ANALYZE on a query and return the execution plan.
   */
  async explainQuery(
    sql: string,
    analyze: boolean = false
  ): Promise<QueryPlan> {
    this.validateReadOnly(sql);

    const explainPrefix = analyze
      ? "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)"
      : "EXPLAIN (FORMAT JSON)";

    const start = performance.now();
    const result = await this.db.queryReadOnly<{ "QUERY PLAN": unknown[] }>(
      `${explainPrefix} ${sql}`
    );
    const executionTimeMs = Math.round(performance.now() - start);

    const planJson = result.rows[0]?.["QUERY PLAN"];
    const plan = Array.isArray(planJson) ? planJson[0] : planJson;
    const planObj = plan as Record<string, unknown>;

    const warnings: string[] = [];

    // Extract warnings from the plan
    this.extractWarnings(planObj, warnings);

    return {
      plan: JSON.stringify(plan, null, 2),
      planningTimeMs: (planObj?.["Planning Time"] as number) ?? 0,
      executionTimeMs,
      warnings,
    };
  }

  /**
   * Get the estimated cost and row counts from a query plan.
   */
  async getQueryCost(sql: string): Promise<{
    estimatedCost: number;
    estimatedRows: number;
    planNodeType: string;
    details: string;
  }> {
    this.validateReadOnly(sql);

    const result = await this.db.queryReadOnly<{ "QUERY PLAN": unknown[] }>(
      `EXPLAIN (FORMAT JSON) ${sql}`
    );

    const planJson = result.rows[0]?.["QUERY PLAN"];
    const plan = (Array.isArray(planJson) ? planJson[0] : planJson) as Record<
      string,
      unknown
    >;
    const topPlan = plan?.["Plan"] as Record<string, unknown>;

    return {
      estimatedCost: (topPlan?.["Total Cost"] as number) ?? 0,
      estimatedRows: (topPlan?.["Plan Rows"] as number) ?? 0,
      planNodeType: (topPlan?.["Node Type"] as string) ?? "Unknown",
      details: JSON.stringify(topPlan, null, 2),
    };
  }

  /**
   * Validate that a SQL query is read-only (no writes, DDL, etc.).
   */
  private validateReadOnly(sql: string): void {
    const forbidden = [
      /\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE)\b/i,
      /\b(COPY)\b/i,
      /\b(VACUUM|REINDEX|CLUSTER)\b/i,
    ];

    const cleaned = sql
      .replace(/--.*$/gm, "")
      .replace(/\/\*[\s\S]*?\*\//g, "");

    for (const pattern of forbidden) {
      if (pattern.test(cleaned)) {
        throw new Error(
          `Query contains forbidden operation. Only SELECT and read-only queries are allowed. ` +
            `Detected pattern: ${pattern.source}`
        );
      }
    }
  }

  /**
   * Ensure a SELECT query has a LIMIT clause to prevent runaway results.
   */
  private ensureLimit(sql: string, limit: number): string {
    const trimmed = sql.trim().replace(/;$/, "");
    const isSelect = /^\s*SELECT/i.test(trimmed);

    if (isSelect && !/\bLIMIT\b/i.test(trimmed)) {
      return `${trimmed} LIMIT ${limit}`;
    }

    return trimmed;
  }

  /**
   * Recursively extract potential performance warnings from a query plan.
   */
  private extractWarnings(
    plan: Record<string, unknown>,
    warnings: string[]
  ): void {
    if (!plan) return;

    const innerPlan = (plan["Plan"] ?? plan) as Record<string, unknown>;
    const nodeType = innerPlan["Node Type"] as string;

    // Warn on sequential scans of large tables
    if (nodeType === "Seq Scan") {
      const rows = (innerPlan["Plan Rows"] as number) ?? 0;
      if (rows > 10000) {
        warnings.push(
          `Sequential scan on "${innerPlan["Relation Name"]}" with ~${rows} estimated rows. Consider adding an index.`
        );
      }
    }

    // Warn on nested loops with high row counts
    if (nodeType === "Nested Loop") {
      const rows = (innerPlan["Plan Rows"] as number) ?? 0;
      if (rows > 50000) {
        warnings.push(
          `Nested Loop join producing ~${rows} rows. This may be slow – consider restructuring the query or adding indexes.`
        );
      }
    }

    // Warn on hash/sort operations spilling to disk
    if (nodeType === "Sort" || nodeType === "Hash") {
      const cost = (innerPlan["Total Cost"] as number) ?? 0;
      if (cost > 10000) {
        warnings.push(
          `High-cost ${nodeType} operation (cost: ${cost}). May benefit from more work_mem or query restructuring.`
        );
      }
    }

    // Recurse into child plans
    const plans = innerPlan["Plans"] as Record<string, unknown>[];
    if (Array.isArray(plans)) {
      for (const child of plans) {
        this.extractWarnings(child, warnings);
      }
    }
  }
}
