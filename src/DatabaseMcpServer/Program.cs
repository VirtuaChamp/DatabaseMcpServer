using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var serviceName = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "DatabaseMcpServer";
var otlpEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole(options =>
{
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName: serviceName, serviceVersion: "1.0.0");

builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(serviceName))
    .WithTracing(tracing =>
    {
        tracing
            .AddSource(Telemetry.ActivitySourceName)
            .AddSqlClientInstrumentation(options =>
            {
                options.SetDbStatementForText = true;
                options.RecordException = true;
            });

        if (!string.IsNullOrEmpty(otlpEndpoint))
        {
            tracing.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
        }

        tracing.AddConsoleExporter();
    })
    .WithMetrics(metrics =>
    {
        metrics.AddMeter(Telemetry.MeterName);

        if (!string.IsNullOrEmpty(otlpEndpoint))
        {
            metrics.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
        }

        metrics.AddConsoleExporter();
    });

builder.Logging.AddOpenTelemetry(logging =>
{
    logging.SetResourceBuilder(resourceBuilder);
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;

    if (!string.IsNullOrEmpty(otlpEndpoint))
    {
        logging.AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint));
    }
});

builder.Services.AddSingleton<SqlCatalog>();
builder.Services.AddSingleton(Telemetry.Instance);

builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();

public sealed class Telemetry
{
    public const string ActivitySourceName = "DatabaseMcpServer";
    public const string MeterName = "DatabaseMcpServer";

    public static readonly Telemetry Instance = new();

    public ActivitySource ActivitySource { get; } = new(ActivitySourceName, "1.0.0");
    public Meter Meter { get; } = new(MeterName, "1.0.0");

    public Counter<long> ToolCallsCounter { get; }
    public Histogram<double> ToolDurationHistogram { get; }
    public Histogram<long> RowsReturnedHistogram { get; }

    private Telemetry()
    {
        ToolCallsCounter = Meter.CreateCounter<long>("mcp.tool.calls", description: "Total tool invocations");
        ToolDurationHistogram = Meter.CreateHistogram<double>("mcp.tool.duration", unit: "s", description: "Tool execution duration");
        RowsReturnedHistogram = Meter.CreateHistogram<long>("mcp.sql.rows_returned", description: "Rows returned per query");
    }
}
