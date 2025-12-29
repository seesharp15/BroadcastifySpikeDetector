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

var cfg = AppConfig.FromEnvironment();

var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

app.MapGet("/api/health", () => Results.Ok(new { ok = true, utc = DateTimeOffset.UtcNow }));

app.MapGet("/api/latest-run", async (int? limit) =>
{
    var take = Clamp(limit ?? 200, 1, 2000);

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
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
  i.listeners,
  i.Rank
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
            rank = r2.GetInt32(5)
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

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
    await conn.OpenAsync();

    var sql = @"
SELECT
  s.ts_utc,
  s.feed_id,
  f.name,
  f.url,
  s.listeners,
  coalesce(s.rank, -1) as ""rank""
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
            rank = r.GetInt32(5),
        });
    }

    return Results.Ok(new { rows });
});

app.MapGet("/api/alerts", async (int? limit) =>
{
    var take = Clamp(limit ?? 300, 1, 5000);

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
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