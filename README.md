# DB Explorer MCP Server

A **Model Context Protocol (MCP)** server that provides AI assistants with powerful tools for PostgreSQL database exploration, query execution, and performance optimization.

## Features

### 🔌 Connection Management
- **connect** – Connect to any PostgreSQL database
- **disconnect** – Safely disconnect
- **connection_status** – Check current connection

### 📊 Schema Exploration
- **list_schemas** – List all schemas in the database
- **list_tables** – List tables in a schema
- **list_views** – List views in a schema
- **describe_table** – Get column names, types, nullability, and defaults
- **get_foreign_keys** – Show foreign key relationships
- **list_indexes** – Show all indexes on a table
- **get_constraints** – Show PRIMARY KEY, UNIQUE, and CHECK constraints
- **table_stats** – Row counts, table/index sizes, vacuum status

### 🔍 Query Execution
- **run_query** – Execute read-only SQL with automatic LIMIT protection
- **explain_query** – EXPLAIN / EXPLAIN ANALYZE with performance warnings
- **query_cost** – Estimate query cost without execution

### ⚡ Performance Optimization
- **suggest_indexes** – Analyze query plans and suggest missing indexes
- **slow_queries** – Find slowest queries via `pg_stat_statements`
- **unused_indexes** – Identify indexes that are never scanned
- **duplicate_indexes** – Find redundant indexes on the same columns
- **table_bloat** – Estimate wasted space from dead tuples
- **database_health** – Overall health: size, connections, cache ratio, transactions

## Safety

All queries run inside **read-only transactions**. Destructive SQL (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE) is **blocked** at the application level before any query reaches the database.

## Quick Start

### Prerequisites
- Node.js 18+
- A running PostgreSQL instance

### Install & Build
```bash
git clone https://github.com/yourusername/db-explorer-mcp.git
cd db-explorer-mcp
npm install
npm run build
```

### Configure with Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "db-explorer": {
      "command": "node",
      "args": ["/absolute/path/to/db-explorer-mcp/dist/index.js"]
    }
  }
}
```

### Configure with VS Code (Copilot)

Add to your `.vscode/mcp.json`:

```json
{
  "servers": {
    "db-explorer": {
      "command": "node",
      "args": ["./db-explorer-mcp/dist/index.js"]
    }
  }
}
```

## Docker

```bash
# Build
docker build -t db-explorer-mcp .

# Run
docker run -i db-explorer-mcp
```

## Usage Examples

Once connected through an MCP client, you can ask the AI:

> "Connect to my local PostgreSQL database and show me all tables"

> "Describe the users table and show its indexes"

> "Run this query and tell me if it's efficient: SELECT * FROM orders WHERE created_at > '2024-01-01'"

> "Suggest indexes to speed up my slow queries"

> "Check the overall health of my database"

## Architecture

```
src/
├── index.ts              # MCP server entry point & tool registration
├── database.ts           # PostgreSQL connection pool manager
└── tools/
    ├── schema.ts         # Schema introspection tools
    ├── query.ts          # Query execution & EXPLAIN analysis
    └── optimization.ts   # Index suggestions, bloat analysis, health checks
```

## Tech Stack

- **TypeScript** – Type-safe implementation
- **MCP SDK** (`@modelcontextprotocol/sdk`) – Model Context Protocol server framework
- **pg** – PostgreSQL client for Node.js
- **Zod** – Runtime schema validation for tool parameters

## License

MIT
