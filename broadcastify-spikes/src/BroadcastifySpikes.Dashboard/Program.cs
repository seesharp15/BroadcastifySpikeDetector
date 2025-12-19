using BroadcastifySpikes.Core;
using Npgsql;

var cfg = AppConfig.FromEnvironment();
var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

var app = WebApplication.CreateBuilder(args).Build();

app.MapGet("/", async () =>
{
    // Super simple embedded HTML page that hits JSON endpoints
    const string html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Broadcastify Spikes</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    .row { display: flex; gap: 24px; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; width: 33%; }
    pre { white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
  <h2>Broadcastify Spikes Dashboard</h2>
  <div class="row">
    <div class="card">
      <h3>Active spikes</h3>
      <pre id="spikes">Loading...</pre>
    </div>
    <div class="card">
      <h3>Recently seen feeds</h3>
      <pre id="seen">Loading...</pre>
    </div>
    <div class="card">
      <h3>Recent samples</h3>
      <pre id="samples">Loading...</pre>
    </div>
  </div>

<script>
async function load() {
  const [spikes, seen, samples] = await Promise.all([
    fetch('/api/active-spikes').then(r => r.json()),
    fetch('/api/recent-seen').then(r => r.json()),
    fetch('/api/recent-samples').then(r => r.json()),
  ]);
  document.getElementById('spikes').textContent = JSON.stringify(spikes, null, 2);
  document.getElementById('seen').textContent = JSON.stringify(seen, null, 2);
  document.getElementById('samples').textContent = JSON.stringify(samples, null, 2);
}
load();
setInterval(load, 5000);
</script>
</body>
</html>
""";
    return Results.Text(html, "text/html");
});

// Active spikes = feeds where spike_state.is_active = true
app.MapGet("/api/active-spikes", async () =>
{
    var cs = cfg.Db.ConnectionString;
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    const string sql = @"
SELECT f.feed_id, f.name, f.url, s.activated_at_utc
FROM spike_state s
JOIN feeds f ON f.feed_id = s.feed_id
WHERE s.is_active = true
ORDER BY s.activated_at_utc DESC
LIMIT 100;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var r = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await r.ReadAsync())
    {
        list.Add(new
        {
            feedId = r.GetString(0),
            name = r.GetString(1),
            url = r.GetString(2),
            activatedAtUtc = r.IsDBNull(3) ? null : r.GetDateTime(3).ToUniversalTime().ToString("O")
        });
    }

    return Results.Json(list);
});

// Recently seen feeds based on last_seen_utc
app.MapGet("/api/recent-seen", async () =>
{
    var cs = cfg.Db.ConnectionString;
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    const string sql = @"
SELECT feed_id, name, url, last_seen_utc, first_seen_utc
FROM feeds
WHERE last_seen_utc IS NOT NULL
ORDER BY last_seen_utc DESC
LIMIT 50;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var r = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await r.ReadAsync())
    {
        list.Add(new
        {
            feedId = r.GetString(0),
            name = r.GetString(1),
            url = r.GetString(2),
            lastSeenUtc = r.IsDBNull(3) ? null : r.GetDateTime(3).ToUniversalTime().ToString("O"),
            firstSeenUtc = r.IsDBNull(4) ? null : r.GetDateTime(4).ToUniversalTime().ToString("O")
        });
    }

    return Results.Json(list);
});

// Recent samples (latest few across all feeds)
app.MapGet("/api/recent-samples", async () =>
{
    var cs = cfg.Db.ConnectionString;
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    const string sql = @"
SELECT s.feed_id, f.name, s.ts_utc, s.listeners
FROM samples s
JOIN feeds f ON f.feed_id = s.feed_id
ORDER BY s.ts_utc DESC
LIMIT 50;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var r = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await r.ReadAsync())
    {
        list.Add(new
        {
            feedId = r.GetString(0),
            name = r.GetString(1),
            tsUtc = r.GetDateTime(2).ToUniversalTime().ToString("O"),
            listeners = r.GetInt32(3)
        });
    }

    return Results.Json(list);
});

app.Run("http://0.0.0.0:8080");
