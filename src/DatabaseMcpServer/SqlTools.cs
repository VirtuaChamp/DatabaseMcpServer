using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using ModelContextProtocol.Server;

[McpServerToolType]
public static partial class SqlTools
{
    private static readonly Regex IdentifierPattern = IdentifierRegex();
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    [GeneratedRegex(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex IdentifierRegex();

    private static string QuoteIdentifier(string name)
    {
        if (!IdentifierPattern.IsMatch(name))
            throw new ArgumentException($"Invalid identifier: {name}");
        return $"[{name}]";
    }

    [McpServerTool, Description("List tables in a schema. Returns a compact JSON array of table names.")]
    public static async Task<string> list_tables(
        SqlCatalog db,
        Telemetry telemetry,
        [Description("Schema name")] string schema = "dbo",
        [Description("Return count only (reduces response size)")] bool countOnly = false)
    {
        using var activity = telemetry.ActivitySource.StartActivity("list_tables");
        activity?.SetTag("mcp.tool.name", "list_tables");
        activity?.SetTag("mcp.schema", schema);
        activity?.SetTag("db.system", "mssql");

        var sw = Stopwatch.StartNew();

        await using var conn = db.Open();
        await conn.OpenAsync();

        if (countOnly)
        {
            const string countSql = """
                SELECT COUNT(*)
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = @schema
                """;
            await using var cmd = new SqlCommand(countSql, conn);
            cmd.Parameters.AddWithValue("@schema", schema);
            var count = (int)(await cmd.ExecuteScalarAsync() ?? 0);

            sw.Stop();
            telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "list_tables"));
            telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "list_tables"));
            activity?.SetTag("mcp.result_count", count);

            return JsonSerializer.Serialize(new { count }, JsonOptions);
        }

        const string sql = """
            SELECT t.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = @schema
            ORDER BY t.name
            """;

        await using var listCmd = new SqlCommand(sql, conn);
        listCmd.Parameters.AddWithValue("@schema", schema);

        var list = new List<string>(128);
        await using var reader = await listCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            list.Add(reader.GetString(0));

        sw.Stop();
        telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "list_tables"));
        telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "list_tables"));
        activity?.SetTag("mcp.result_count", list.Count);

        return JsonSerializer.Serialize(list, JsonOptions);
    }

    [McpServerTool, Description("Describe table columns. Returns compact JSON: {\"c\":[[name,type,isNullable,maxLen,prec,scale],...]}")]
    public static async Task<string> describe_table(
        SqlCatalog db,
        Telemetry telemetry,
        [Description("Table name")] string table,
        [Description("Schema name")] string schema = "dbo")
    {
        using var activity = telemetry.ActivitySource.StartActivity("describe_table");
        activity?.SetTag("mcp.tool.name", "describe_table");
        activity?.SetTag("mcp.schema", schema);
        activity?.SetTag("mcp.table", table);
        activity?.SetTag("db.system", "mssql");

        var sw = Stopwatch.StartNew();

        const string sql = """
            SELECT
                c.name AS ColName,
                ty.name AS TypeName,
                c.is_nullable,
                c.max_length,
                c.precision,
                c.scale
            FROM sys.columns c
            JOIN sys.tables t ON t.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.types ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = @schema AND t.name = @table
            ORDER BY c.column_id
            """;

        await using var conn = db.Open();
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        var cols = new List<object[]>(64);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            cols.Add(
            [
                reader.GetString(0),
                reader.GetString(1),
                reader.GetBoolean(2) ? 1 : 0,
                reader.GetInt16(3),
                reader.GetByte(4),
                reader.GetByte(5)
            ]);
        }

        sw.Stop();
        telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "describe_table"));
        telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "describe_table"));
        activity?.SetTag("mcp.result_count", cols.Count);

        return JsonSerializer.Serialize(new { c = cols }, JsonOptions);
    }

    [McpServerTool, Description("Sample rows from a table (read-only). Output is token-optimized: cols once + row arrays. Hard-capped.")]
    public static async Task<string> sample_rows(
        SqlCatalog db,
        Telemetry telemetry,
        [Description("Table name")] string table,
        [Description("Schema name")] string schema = "dbo",
        [Description("Max rows (1-200)")] int limit = 20,
        [Description("Specific columns (optional)")] string[]? columns = null,
        [Description("Max chars per cell (20-2000)")] int maxCellChars = 200)
    {
        using var activity = telemetry.ActivitySource.StartActivity("sample_rows");
        activity?.SetTag("mcp.tool.name", "sample_rows");
        activity?.SetTag("mcp.schema", schema);
        activity?.SetTag("mcp.table", table);
        activity?.SetTag("db.system", "mssql");

        var sw = Stopwatch.StartNew();

        limit = Math.Clamp(limit, 1, 200);
        maxCellChars = Math.Clamp(maxCellChars, 20, 2000);

        var s = QuoteIdentifier(schema);
        var t = QuoteIdentifier(table);

        var colList = columns is { Length: > 0 }
            ? string.Join(",", columns.Select(QuoteIdentifier))
            : "*";

        var sql = $"SELECT TOP (@limit) {colList} FROM {s}.{t}";

        await using var conn = db.Open();
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess);

        var colNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        var rows = new List<object?[]>(limit);

        while (await reader.ReadAsync())
        {
            var row = new object?[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (await reader.IsDBNullAsync(i))
                {
                    row[i] = null;
                    continue;
                }

                var val = reader.GetValue(i);

                if (val is string str && str.Length > maxCellChars)
                    val = string.Concat(str.AsSpan(0, maxCellChars), "...");
                else if (val is byte[] bytes)
                    val = $"[binary:{bytes.Length}]";

                row[i] = val;
            }
            rows.Add(row);
        }

        sw.Stop();
        telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "sample_rows"));
        telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "sample_rows"));
        telemetry.RowsReturnedHistogram.Record(rows.Count, new KeyValuePair<string, object?>("tool", "sample_rows"));
        activity?.SetTag("mcp.result_count", rows.Count);

        return JsonSerializer.Serialize(new { cols = colNames, rows }, JsonOptions);
    }

    [McpServerTool, Description("List stored procedures in a schema. Returns a compact JSON array of procedure names.")]
    public static async Task<string> list_procedures(
        SqlCatalog db,
        Telemetry telemetry,
        [Description("Schema name")] string schema = "dbo",
        [Description("Return count only (reduces response size)")] bool countOnly = false)
    {
        using var activity = telemetry.ActivitySource.StartActivity("list_procedures");
        activity?.SetTag("mcp.tool.name", "list_procedures");
        activity?.SetTag("mcp.schema", schema);
        activity?.SetTag("db.system", "mssql");

        var sw = Stopwatch.StartNew();

        await using var conn = db.Open();
        await conn.OpenAsync();

        if (countOnly)
        {
            const string countSql = """
                SELECT COUNT(*)
                FROM sys.procedures p
                JOIN sys.schemas s ON s.schema_id = p.schema_id
                WHERE s.name = @schema
                """;
            await using var cmd = new SqlCommand(countSql, conn);
            cmd.Parameters.AddWithValue("@schema", schema);
            var count = (int)(await cmd.ExecuteScalarAsync() ?? 0);

            sw.Stop();
            telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "list_procedures"));
            telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "list_procedures"));
            activity?.SetTag("mcp.result_count", count);

            return JsonSerializer.Serialize(new { count }, JsonOptions);
        }

        const string sql = """
            SELECT p.name
            FROM sys.procedures p
            JOIN sys.schemas s ON s.schema_id = p.schema_id
            WHERE s.name = @schema
            ORDER BY p.name
            """;

        await using var listCmd = new SqlCommand(sql, conn);
        listCmd.Parameters.AddWithValue("@schema", schema);

        var list = new List<string>(256);
        await using var reader = await listCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            list.Add(reader.GetString(0));

        sw.Stop();
        telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "list_procedures"));
        telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "list_procedures"));
        activity?.SetTag("mcp.result_count", list.Count);

        return JsonSerializer.Serialize(list, JsonOptions);
    }

    [McpServerTool, Description("Read stored procedure definition text (no execute). Returns a truncated string by default.")]
    public static async Task<string> get_procedure_definition(
        SqlCatalog db,
        Telemetry telemetry,
        [Description("Procedure name")] string procedure,
        [Description("Schema name")] string schema = "dbo",
        [Description("Max chars (200-20000)")] int maxChars = 4000,
        [Description("Char offset for pagination")] int offset = 0)
    {
        using var activity = telemetry.ActivitySource.StartActivity("get_procedure_definition");
        activity?.SetTag("mcp.tool.name", "get_procedure_definition");
        activity?.SetTag("mcp.schema", schema);
        activity?.SetTag("mcp.procedure", procedure);
        activity?.SetTag("db.system", "mssql");

        var sw = Stopwatch.StartNew();

        maxChars = Math.Clamp(maxChars, 200, 20000);
        offset = Math.Max(0, offset);

        var fullName = $"{QuoteIdentifier(schema)}.{QuoteIdentifier(procedure)}";
        var sql = $"SELECT OBJECT_DEFINITION(OBJECT_ID(N'{fullName}'))";

        await using var conn = db.Open();
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);

        var def = (string?)await cmd.ExecuteScalarAsync() ?? "";

        sw.Stop();
        telemetry.ToolCallsCounter.Add(1, new KeyValuePair<string, object?>("tool", "get_procedure_definition"));
        telemetry.ToolDurationHistogram.Record(sw.Elapsed.TotalSeconds, new KeyValuePair<string, object?>("tool", "get_procedure_definition"));
        activity?.SetTag("mcp.definition_length", def.Length);

        if (offset >= def.Length)
            return "";

        var sliceLen = Math.Min(maxChars, def.Length - offset);
        return def.Substring(offset, sliceLen);
    }
}
