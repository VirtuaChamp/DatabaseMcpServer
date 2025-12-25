using Microsoft.Data.SqlClient;

public sealed class SqlCatalog
{
    private readonly string _connectionString;

    public SqlCatalog()
    {
        _connectionString = Environment.GetEnvironmentVariable("SQLSERVER_CONNSTR")
            ?? throw new InvalidOperationException("SQLSERVER_CONNSTR environment variable is required");
    }

    public SqlConnection Open()
    {
        return new SqlConnection(_connectionString);
    }
}
