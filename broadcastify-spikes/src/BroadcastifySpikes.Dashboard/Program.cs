using BroadcastifySpikes.Core;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRouting();
builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

// Prefer AppConfig (same pattern as other services). This should build from existing env vars like
// POSTGRES_HOST / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB (or whatever your AppConfig expects).
string? cs = null;

try
{
    var cfg = AppConfig.FromEnvironment();
    cs = cfg.Db.ConnectionString;
}
catch
{
    // Swallow and fallback to explicit connection string env vars below.
}

// Fallback if AppConfig didn't provide a usable connection string
cs ??= Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING")
   ?? Environment.GetEnvironmentVariable("DB_CONNECTION_STRING");

if (string.IsNullOrWhiteSpace(cs))
{
    throw new InvalidOperationException(
        "Dashboard could not determine Postgres connection string. " +
        "Either ensure AppConfig env vars are set (POSTGRES_HOST/USER/PASSWORD/DB etc.), " +
        "or set POSTGRES_CONNECTION_STRING (or DB_CONNECTION_STRING).");
}

// Ensure dashboard-required tables exist (idempotent)
await EnsureSchemaAsync(cs);

app.MapGet("/api/health", () => Results.Ok(new { ok = true, utc = DateTimeOffset.UtcNow }));

app.MapGet("/api/latest-run", async (int? limit) =>
{
    var take = Clamp(limit ?? 200, 1, 2000);

    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var runSql = @"
SELECT run_id, started_at_utc, completed_at_utc, pulled_count
FROM ingest_runs
ORDER BY COALESCE(completed_at_utc, started_at_utc) DESC
LIMIT 1;";

    await using var runCmd = new NpgsqlCommand(runSql, conn);
    await using var rr = await runCmd.ExecuteReaderAsync();

    if (!await rr.ReadAsync())
    {
        return Results.Ok(new { run = (object?)null, records = Array.Empty<object>() });
    }

    var runId = rr.GetGuid(0);
    var started = rr.GetDateTime(1);
    var completed = rr.IsDBNull(2) ? (DateTime?)null : rr.GetDateTime(2);
    var pulledCount = rr.IsDBNull(3) ? 0 : rr.GetInt32(3);
    await rr.CloseAsync();

    var recSql = @"
SELECT
  i.ts_utc,
  i.feed_id,
  f.name,
  f.url,
  i.listeners
FROM ingest_run_items i
JOIN feeds f ON f.feed_id = i.feed_id
WHERE i.run_id = @rid
ORDER BY i.listeners DESC, i.ts_utc DESC
LIMIT @lim;";

    await using var recCmd = new NpgsqlCommand(recSql, conn);
    recCmd.Parameters.AddWithValue("@rid", runId);
    recCmd.Parameters.AddWithValue("@lim", take);

    var records = new List<object>(take);
    await using var r2 = await recCmd.ExecuteReaderAsync();
    while (await r2.ReadAsync())
    {
        records.Add(new
        {
            tsUtc = new DateTimeOffset(r2.GetDateTime(0), TimeSpan.Zero),
            feedId = r2.GetString(1),
            name = r2.GetString(2),
            url = r2.GetString(3),
            listeners = r2.GetInt32(4),
        });
    }

    return Results.Ok(new
    {
        run = new
        {
            runId,
            startedAtUtc = new DateTimeOffset(started, TimeSpan.Zero),
            completedAtUtc = completed ?? new DateTimeOffset(completed.Value, TimeSpan.Zero),
            pulledCount
        },
        records
    });
});

app.MapGet("/api/samples", async (int? limit) =>
{
    var take = Clamp(limit ?? 400, 1, 5000);

    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
SELECT
  s.ts_utc,
  s.feed_id,
  f.name,
  f.url,
  s.listeners
FROM samples s
JOIN feeds f ON f.feed_id = s.feed_id
ORDER BY s.ts_utc DESC
LIMIT @lim;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@lim", take);

    var rows = new List<object>(take);
    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        rows.Add(new
        {
            tsUtc = new DateTimeOffset(r.GetDateTime(0), TimeSpan.Zero),
            feedId = r.GetString(1),
            name = r.GetString(2),
            url = r.GetString(3),
            listeners = r.GetInt32(4),
        });
    }

    return Results.Ok(new { rows });
});

app.MapGet("/api/alerts", async (int? limit) =>
{
    var take = Clamp(limit ?? 300, 1, 5000);

    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
SELECT
  a.ts_utc,
  a.feed_id,
  f.name,
  f.url,
  a.alert_type,
  a.message
FROM alert_history a
JOIN feeds f ON f.feed_id = a.feed_id
ORDER BY a.ts_utc DESC
LIMIT @lim;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@lim", take);

    var rows = new List<object>(take);
    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        rows.Add(new
        {
            tsUtc = new DateTimeOffset(r.GetDateTime(0), TimeSpan.Zero),
            feedId = r.GetString(1),
            name = r.GetString(2),
            url = r.GetString(3),
            alertType = r.GetString(4),
            message = r.IsDBNull(5) ? "" : r.GetString(5),
        });
    }

    return Results.Ok(new { rows });
});

app.Run();

static int Clamp(int v, int min, int max)
{
    return v < min ? min : (v > max ? max : v);
}

static async Task EnsureSchemaAsync(string cs)
{
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id UUID PRIMARY KEY,
  started_at_utc TIMESTAMPTZ NOT NULL,
  completed_at_utc TIMESTAMPTZ NULL,
  pulled_count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingest_run_items (
  run_id UUID NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
  ts_utc TIMESTAMPTZ NOT NULL,
  listeners INT NOT NULL,
  PRIMARY KEY (run_id, feed_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_run_items_ts ON ingest_run_items(ts_utc);
CREATE INDEX IF NOT EXISTS idx_ingest_runs_completed ON ingest_runs(completed_at_utc);

CREATE TABLE IF NOT EXISTS alert_history (
  id BIGSERIAL PRIMARY KEY,
  ts_utc TIMESTAMPTZ NOT NULL,
  feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
  alert_type TEXT NOT NULL,
  message TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_history_ts ON alert_history(ts_utc);
";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await cmd.ExecuteNonQueryAsync();
}
