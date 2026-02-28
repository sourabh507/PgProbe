#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { DatabaseManager } from "./database.js";
import { SchemaTools } from "./tools/schema.js";
import { QueryTools } from "./tools/query.js";
import { OptimizationTools } from "./tools/optimization.js";

// ─── Instantiate core services ───────────────────────────────────────────────

const db = new DatabaseManager();
const schemaTools = new SchemaTools(db);
const queryTools = new QueryTools(db);
const optimizationTools = new OptimizationTools(db);

// ─── Create MCP Server ──────────────────────────────────────────────────────

const server = new McpServer({
  name: "db-explorer",
  version: "1.0.0",
  description:
    "A database exploration and query optimization MCP server for PostgreSQL",
});

// ─── Helper ──────────────────────────────────────────────────────────────────

function formatResult(data: unknown): string {
  return JSON.stringify(data, null, 2);
}

function errorResult(error: unknown): { content: { type: "text"; text: string }[] } {
  const message = error instanceof Error ? error.message : String(error);
  return {
    content: [{ type: "text" as const, text: `Error: ${message}` }],
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONNECTION TOOLS
// ═══════════════════════════════════════════════════════════════════════════════

server.tool(
  "connect",
  "Connect to a PostgreSQL database. Must be called before using any other tool.",
  {
    host: z.string().describe("Database host (e.g. localhost)"),
    port: z.number().default(5432).describe("Database port"),
    database: z.string().describe("Database name"),
    user: z.string().describe("Database user"),
    password: z.string().describe("Database password"),
    ssl: z.boolean().default(false).describe("Use SSL connection"),
  },
  async (params) => {
    try {
      await db.connect(params);
      return {
        content: [
          {
            type: "text" as const,
            text: `Successfully connected to PostgreSQL at ${params.host}:${params.port}/${params.database}`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "disconnect",
  "Disconnect from the current database.",
  {},
  async () => {
    try {
      await db.disconnect();
      return {
        content: [{ type: "text" as const, text: "Disconnected successfully." }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "connection_status",
  "Check current database connection status.",
  {},
  async () => {
    const info = db.getConnectionInfo();
    if (!info) {
      return {
        content: [
          { type: "text" as const, text: "Not connected to any database." },
        ],
      };
    }
    return {
      content: [
        {
          type: "text" as const,
          text: `Connected to ${info.host}:${info.port}/${info.database} as ${info.user}`,
        },
      ],
    };
  }
);

// ═══════════════════════════════════════════════════════════════════════════════
// SCHEMA EXPLORATION TOOLS
// ═══════════════════════════════════════════════════════════════════════════════

server.tool(
  "list_schemas",
  "List all user-defined schemas in the current database.",
  {},
  async () => {
    try {
      const schemas = await schemaTools.listSchemas();
      return {
        content: [{ type: "text" as const, text: formatResult(schemas) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "list_tables",
  "List all tables in a given schema.",
  {
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ schema }) => {
    try {
      const tables = await schemaTools.listTables(schema);
      return {
        content: [
          {
            type: "text" as const,
            text: tables.length > 0
              ? formatResult(tables)
              : `No tables found in schema "${schema}".`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "list_views",
  "List all views in a given schema.",
  {
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ schema }) => {
    try {
      const views = await schemaTools.listViews(schema);
      return {
        content: [
          {
            type: "text" as const,
            text: views.length > 0
              ? formatResult(views)
              : `No views found in schema "${schema}".`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "describe_table",
  "Describe a table's columns, data types, nullability, and defaults.",
  {
    table: z.string().describe("Table name"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ table, schema }) => {
    try {
      const columns = await schemaTools.describeTable(table, schema);
      return {
        content: [{ type: "text" as const, text: formatResult(columns) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "get_foreign_keys",
  "Get all foreign key relationships for a table.",
  {
    table: z.string().describe("Table name"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ table, schema }) => {
    try {
      const fks = await schemaTools.getForeignKeys(table, schema);
      return {
        content: [
          {
            type: "text" as const,
            text: fks.length > 0
              ? formatResult(fks)
              : `No foreign keys found on "${schema}.${table}".`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "list_indexes",
  "List all indexes on a table, including type, uniqueness, and columns.",
  {
    table: z.string().describe("Table name"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ table, schema }) => {
    try {
      const indexes = await schemaTools.listIndexes(table, schema);
      return {
        content: [
          {
            type: "text" as const,
            text: indexes.length > 0
              ? formatResult(indexes)
              : `No indexes found on "${schema}.${table}".`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "get_constraints",
  "Get all PRIMARY KEY, UNIQUE, and CHECK constraints for a table.",
  {
    table: z.string().describe("Table name"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ table, schema }) => {
    try {
      const constraints = await schemaTools.getConstraints(table, schema);
      return {
        content: [{ type: "text" as const, text: formatResult(constraints) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "table_stats",
  "Get table statistics: row counts, sizes, vacuum status, and dead tuples.",
  {
    table: z.string().describe("Table name"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ table, schema }) => {
    try {
      const stats = await schemaTools.getTableStats(table, schema);
      return {
        content: [
          {
            type: "text" as const,
            text: stats
              ? formatResult(stats)
              : `Table "${schema}.${table}" not found.`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════════
// QUERY EXECUTION TOOLS
// ═══════════════════════════════════════════════════════════════════════════════

server.tool(
  "run_query",
  "Execute a read-only SQL query. Destructive operations (INSERT, UPDATE, DELETE, DROP, etc.) are blocked.",
  {
    sql: z.string().describe("SQL query to execute (SELECT only)"),
    limit: z
      .number()
      .default(100)
      .describe("Maximum rows to return (default: 100)"),
  },
  async ({ sql, limit }) => {
    try {
      const result = await queryTools.executeQuery(sql, undefined, limit);
      const output = [
        `Columns: ${result.columns.join(", ")}`,
        `Rows returned: ${result.rowCount}`,
        `Execution time: ${result.executionTimeMs}ms`,
        "",
        formatResult(result.rows),
      ].join("\n");

      return {
        content: [{ type: "text" as const, text: output }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "explain_query",
  "Show the execution plan for a SQL query (EXPLAIN). Helps identify performance bottlenecks.",
  {
    sql: z.string().describe("SQL query to analyze"),
    analyze: z
      .boolean()
      .default(false)
      .describe(
        "If true, actually executes the query to get real timing (EXPLAIN ANALYZE)"
      ),
  },
  async ({ sql, analyze }) => {
    try {
      const plan = await queryTools.explainQuery(sql, analyze);
      const output = [
        `Planning time: ${plan.planningTimeMs}ms`,
        `Execution time: ${plan.executionTimeMs}ms`,
        plan.warnings.length > 0
          ? `\n⚠ Warnings:\n${plan.warnings.map((w) => `  • ${w}`).join("\n")}`
          : "",
        `\nQuery Plan:\n${plan.plan}`,
      ].join("\n");

      return {
        content: [{ type: "text" as const, text: output }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "query_cost",
  "Get the estimated cost and row count for a SQL query without executing it.",
  {
    sql: z.string().describe("SQL query to estimate cost for"),
  },
  async ({ sql }) => {
    try {
      const cost = await queryTools.getQueryCost(sql);
      return {
        content: [{ type: "text" as const, text: formatResult(cost) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════════
// OPTIMIZATION TOOLS
// ═══════════════════════════════════════════════════════════════════════════════

server.tool(
  "suggest_indexes",
  "Analyze a SQL query's execution plan and suggest indexes to improve performance.",
  {
    sql: z.string().describe("SQL query to analyze for index suggestions"),
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ sql, schema }) => {
    try {
      const suggestions = await optimizationTools.suggestIndexes(sql, schema);

      if (suggestions.length === 0) {
        return {
          content: [
            {
              type: "text" as const,
              text: "No index suggestions for this query. The query plan looks optimal.",
            },
          ],
        };
      }

      const output = suggestions
        .map(
          (s, i) =>
            `${i + 1}. [${s.impact.toUpperCase()}] ${s.table}\n` +
            `   Columns: ${s.columns.join(", ")}\n` +
            `   Reason: ${s.reason}\n` +
            `   SQL: ${s.createStatement}`
        )
        .join("\n\n");

      return {
        content: [
          {
            type: "text" as const,
            text: `Index Suggestions:\n\n${output}`,
          },
        ],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "slow_queries",
  "Find the slowest queries using pg_stat_statements (requires the extension to be installed).",
  {
    limit: z
      .number()
      .default(10)
      .describe("Number of slow queries to return (default: 10)"),
  },
  async ({ limit }) => {
    try {
      const queries = await optimizationTools.getSlowQueries(limit);
      return {
        content: [{ type: "text" as const, text: formatResult(queries) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "unused_indexes",
  "Find indexes that have never been used (candidates for removal to save space and write overhead).",
  {
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ schema }) => {
    try {
      const indexes = await optimizationTools.findUnusedIndexes(schema);

      if (indexes.length === 0) {
        return {
          content: [
            {
              type: "text" as const,
              text: "No unused indexes found. All indexes appear to be in use.",
            },
          ],
        };
      }

      return {
        content: [{ type: "text" as const, text: formatResult(indexes) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "duplicate_indexes",
  "Find duplicate indexes (multiple indexes covering the same columns on the same table).",
  {
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ schema }) => {
    try {
      const duplicates = await optimizationTools.findDuplicateIndexes(schema);

      if (duplicates.length === 0) {
        return {
          content: [
            {
              type: "text" as const,
              text: "No duplicate indexes found.",
            },
          ],
        };
      }

      return {
        content: [{ type: "text" as const, text: formatResult(duplicates) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "table_bloat",
  "Estimate table bloat – wasted space from dead tuples that can be reclaimed with VACUUM.",
  {
    schema: z
      .string()
      .default("public")
      .describe("Schema name (default: public)"),
  },
  async ({ schema }) => {
    try {
      const bloat = await optimizationTools.getTableBloat(schema);
      return {
        content: [{ type: "text" as const, text: formatResult(bloat) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

server.tool(
  "database_health",
  "Get an overview of database health: size, connections, cache hit ratio, and transaction counts.",
  {},
  async () => {
    try {
      const health = await optimizationTools.getDatabaseHealth();
      return {
        content: [{ type: "text" as const, text: formatResult(health) }],
      };
    } catch (error) {
      return errorResult(error);
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════════
// RESOURCES (Contextual database info exposed via MCP resources)
// ═══════════════════════════════════════════════════════════════════════════════

server.resource(
  "database-overview",
  "db://overview",
  async (uri) => {
    if (!db.isConnected()) {
      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "text/plain",
            text: "Not connected to a database. Use the 'connect' tool first.",
          },
        ],
      };
    }

    try {
      const schemas = await schemaTools.listSchemas();
      const tablesPerSchema: Record<string, string[]> = {};

      for (const schema of schemas) {
        tablesPerSchema[schema] = await schemaTools.listTables(schema);
      }

      const overview = {
        connection: db.getConnectionInfo(),
        schemas: tablesPerSchema,
      };

      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(overview, null, 2),
          },
        ],
      };
    } catch (error) {
      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "text/plain",
            text: `Error generating overview: ${error instanceof Error ? error.message : String(error)}`,
          },
        ],
      };
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("DB Explorer MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
