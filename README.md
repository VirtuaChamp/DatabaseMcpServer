# DatabaseMcpServer

A read-only SQL Server MCP (Model Context Protocol) server for AI assistants.

## Tools

- `list_tables` - List tables in a schema
- `describe_table` - Get column definitions for a table
- `sample_rows` - Sample data from a table (capped, token-optimized)
- `list_procedures` - List stored procedures in a schema
- `get_procedure_definition` - Read stored procedure source code

## Setup

1. Copy `.mcp.json.example` to `.mcp.json`
2. Set `SQLSERVER_CONNSTR` with your connection string
3. Run: `dotnet run --project src/DatabaseMcpServer/DatabaseMcpServer.csproj`

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SQLSERVER_CONNSTR` | SQL Server connection string (required) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry endpoint (optional) |
| `OTEL_SERVICE_NAME` | Service name for telemetry (optional) |
