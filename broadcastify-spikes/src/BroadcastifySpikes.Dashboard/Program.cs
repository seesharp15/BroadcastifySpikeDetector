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
            completedAtUtc = completed.HasValue ? new DateTimeOffset(completed.Value, TimeSpan.Zero) : (DateTimeOffset?)null,

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

app.MapGet("/api/stream-history", async (string feedId, int? limit) =>
{
    if (string.IsNullOrWhiteSpace(feedId))
        return Results.BadRequest(new { error = "feedId is required" });

    var take = Clamp(limit ?? 2000, 1, 20000);

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
    await conn.OpenAsync();

    // Most recent samples for that feed
    var sql = @"
SELECT
  s.ts_utc,
  s.listeners,
  COALESCE(s.rank, -1) AS rank,
  f.url
FROM samples s
JOIN feeds f ON f.feed_id = s.feed_id
WHERE s.feed_id = @fid
ORDER BY s.ts_utc DESC
LIMIT @lim;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("@fid", feedId);
    cmd.Parameters.AddWithValue("@lim", take);

    var rows = new List<object>(take);
    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        rows.Add(new
        {
            tsUtc = new DateTimeOffset(r.GetDateTime(0), TimeSpan.Zero),
            listeners = r.GetInt32(1),
            rank = r.GetInt32(2),
            url = r.GetString(3),
        });
    }

    return Results.Ok(new { feedId, rows });
});

app.MapGet("/api/global-stats", async (string feedId, int? lookbackHours) =>
{
    if (string.IsNullOrWhiteSpace(feedId))
        return Results.BadRequest(new { error = "feedId is required" });

    var hours = Clamp(lookbackHours ?? 24, 1, 24 * 30); // 1h..30d
    var sinceUtc = DateTime.UtcNow.AddHours(-hours);

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
    await conn.OpenAsync();

    // Global stats across all feeds over the lookback window
    // (Using Postgres aggregate functions; percentile_cont gives P95)
    var globalSql = @"
SELECT
  COUNT(*)::bigint AS n,
  AVG(s.listeners)::double precision AS mean,
  STDDEV_POP(s.listeners)::double precision AS stddev,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY s.listeners)::double precision AS p95,
  MIN(s.listeners) AS min,
  MAX(s.listeners) AS max
FROM samples s
WHERE s.ts_utc >= @since;";

    long gN;
    double? gMean, gStd, gP95;
    int? gMin, gMax;

    await using (var cmd = new NpgsqlCommand(globalSql, conn))
    {
        cmd.Parameters.AddWithValue("@since", sinceUtc);

        await using var r = await cmd.ExecuteReaderAsync();
        await r.ReadAsync();

        gN = r.IsDBNull(0) ? 0 : r.GetInt64(0);
        gMean = r.IsDBNull(1) ? null : r.GetDouble(1);
        gStd = r.IsDBNull(2) ? null : r.GetDouble(2);
        gP95 = r.IsDBNull(3) ? null : r.GetDouble(3);
        gMin = r.IsDBNull(4) ? null : r.GetInt32(4);
        gMax = r.IsDBNull(5) ? null : r.GetInt32(5);
    }

    // Per-feed stats over the lookback window (this is what you use to decide spike-iness)
    var feedSql = @"
SELECT
  COUNT(*)::bigint AS n,
  AVG(s.listeners)::double precision AS mean,
  STDDEV_POP(s.listeners)::double precision AS stddev,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY s.listeners)::double precision AS p95,
  MIN(s.listeners) AS min,
  MAX(s.listeners) AS max
FROM samples s
WHERE s.feed_id = @fid
  AND s.ts_utc >= @since;";

    long fN;
    double? fMean, fStd, fP95;
    int? fMin, fMax;

    await using (var cmd = new NpgsqlCommand(feedSql, conn))
    {
        cmd.Parameters.AddWithValue("@fid", feedId);
        cmd.Parameters.AddWithValue("@since", sinceUtc);

        await using var r = await cmd.ExecuteReaderAsync();
        await r.ReadAsync();

        fN = r.IsDBNull(0) ? 0 : r.GetInt64(0);
        fMean = r.IsDBNull(1) ? null : r.GetDouble(1);
        fStd = r.IsDBNull(2) ? null : r.GetDouble(2);
        fP95 = r.IsDBNull(3) ? null : r.GetDouble(3);
        fMin = r.IsDBNull(4) ? null : r.GetInt32(4);
        fMax = r.IsDBNull(5) ? null : r.GetInt32(5);
    }

    // A simple “reference” threshold so you can eyeball expected spike behavior.
    // Tune this to match your detector logic if you have a different formula.
    double? threshold = (fMean.HasValue && fStd.HasValue)
        ? (fMean.Value + (3.0 * fStd.Value))
        : null;

    return Results.Ok(new
    {
        feedId,
        window = new { lookbackHours = hours, sinceUtc = new DateTimeOffset(sinceUtc, TimeSpan.Zero) },
        global = new { n = gN, mean = gMean, stdDev = gStd, p95 = gP95, min = gMin, max = gMax },
        feed = new { n = fN, mean = fMean, stdDev = fStd, p95 = fP95, min = fMin, max = fMax, threshold },
        hint = "feed.threshold is mean + 3*stdDev over the lookback window; adjust to match detector logic."
    });
});

app.MapGet("/api/inspect-feed", async (string feedId) =>
{
    if (string.IsNullOrWhiteSpace(feedId))
        return Results.BadRequest(new { error = "feedId is required" });

    var now = DateTimeOffset.UtcNow;

    var fromUtc = now - cfg.Detect.LookbackWindow;
    var globalFromUtc = now - cfg.Detect.GlobalLookbackWindow;

    await using var conn = new NpgsqlConnection(cfg.Db.ConnectionString);
    await conn.OpenAsync();

    // ---- Feed metadata ----
    string? feedName = null;
    string? feedUrl = null;

    await using (var cmd = new NpgsqlCommand(@"
SELECT name, url
FROM feeds
WHERE feed_id = @fid
LIMIT 1;", conn))
    {
        cmd.Parameters.AddWithValue("@fid", feedId);
        await using var r = await cmd.ExecuteReaderAsync();
        if (await r.ReadAsync())
        {
            feedName = r.GetString(0);
            feedUrl = r.GetString(1);
        }
    }

    // ---- Per-feed samples (ascending so "current" == last) ----
    var samples = new List<(DateTimeOffset tsUtc, int listeners, int? rank)>(4096);

    await using (var cmd = new NpgsqlCommand(@"
SELECT ts_utc, listeners, rank
FROM samples
WHERE feed_id = @fid
  AND ts_utc >= @since
ORDER BY ts_utc ASC;", conn))
    {
        cmd.Parameters.AddWithValue("@fid", feedId);
        cmd.Parameters.AddWithValue("@since", fromUtc.UtcDateTime);

        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var ts = new DateTimeOffset(r.GetDateTime(0), TimeSpan.Zero);
            var listeners = r.GetInt32(1);
            int? rank = r.IsDBNull(2) ? null : r.GetInt32(2);
            samples.Add((ts, listeners, rank));
        }
    }

    if (samples.Count == 0)
    {
        return Results.Ok(new
        {
            feedId,
            name = feedName,
            url = feedUrl,
            nowUtc = now,
            reason = "No samples in lookback window.",
            decision = new { isSpikeNow = false, isRecovered = false, baseline = "none" }
        });
    }

    var current = samples[^1];
    var sampleAge = now - current.tsUtc;
    var perFeedAllowed = sampleAge <= cfg.Detect.MaxSampleAge;

    // ---- Per-feed baseline per detector: baseline = all but last PersistSamples ----
    var hasPerFeedBaseline = false;
    double median = 0, mad = 0, currentZ = 0;

    double[]? recentZ = null;

    if (perFeedAllowed &&
        samples.Count >= cfg.Detect.MinSamples &&
        samples.Count >= cfg.Detect.PersistSamples + 2)
    {
        var n = cfg.Detect.PersistSamples;
        var baseline = samples.Take(samples.Count - n)
                              .Select(s => (double)s.listeners)
                              .ToArray();

        if (baseline.Length >= cfg.Detect.MinSamples)
        {
            median = Median(baseline);
            mad = MAD(baseline, median);

            if (mad > 1e-9)
            {
                currentZ = RobustZ(current.listeners, median, mad);
                hasPerFeedBaseline = true;

                var recent = samples.Skip(samples.Count - n).Select(s => (double)s.listeners).ToArray();
                recentZ = recent.Select(x => RobustZ(x, median, mad)).ToArray();
            }
        }
    }

    // ---- Global bucket fallback per detector ----
    // Pull global rank samples ONCE and compute bucket stats (bucketSize=5)
    var globalRankSamples = new List<(int rank, int listeners)>(50_000);

    await using (var cmd = new NpgsqlCommand(@"
SELECT rank, listeners
FROM samples
WHERE ts_utc >= @since
  AND rank IS NOT NULL
  AND rank >= 1
  AND rank <= 25;", conn))
    {
        cmd.Parameters.AddWithValue("@since", globalFromUtc.UtcDateTime);
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            globalRankSamples.Add((r.GetInt32(0), r.GetInt32(1)));
        }
    }

    var globalBucketStats = ComputeGlobalBucketStats(globalRankSamples, bucketSize: 5);

    // ---- Decide spike/recovered exactly like detector ----
    var isSpikeNow = false;
    var isRecovered = false;

    string baselineUsed;
    int? bucketUsed = null;
    object? bucketStatsUsed = null;
    bool inferredBucket = false;

    if (hasPerFeedBaseline)
    {
        baselineUsed = "per_feed";

        // Persist window rule: all recent robust-Z must be >= threshold
        if (recentZ is not null)
            isSpikeNow = recentZ.All(z => z >= cfg.Detect.RobustZThreshold);

        isRecovered = currentZ <= cfg.Detect.RecoveryZThreshold;
    }
    else
    {
        baselineUsed = "global_bucket";

        var rank = current.rank;
        var bucket = rank is not null
            ? (Math.Clamp(rank.Value, 1, 25) - 1) / 5
            : InferBucketFromListeners(current.listeners, globalBucketStats);

        inferredBucket = rank is null;
        bucketUsed = bucket;

        if (globalBucketStats.TryGetValue(bucket, out var gs) &&
            gs.SampleCount >= cfg.Detect.GlobalMinSamples &&
            gs.Mad > 1e-9)
        {
            median = gs.Median;
            mad = gs.Mad;
            currentZ = RobustZ(current.listeners, median, mad);

            isSpikeNow =
                currentZ >= cfg.Detect.GlobalRobustZThreshold &&
                current.listeners >= cfg.Detect.NewFeedMinListeners;

            isRecovered = currentZ <= cfg.Detect.RecoveryZThreshold;

            bucketStatsUsed = new
            {
                bucket,
                gs.SampleCount,
                median = gs.Median,
                mad = gs.Mad
            };
        }
        else
        {
            baselineUsed = "hard_threshold";

            isSpikeNow = current.listeners >= cfg.Detect.NewFeedMinListeners;
            isRecovered = current.listeners < cfg.Detect.NewFeedMinListeners;
        }
    }

    // Helpful: what the detector thresholds are
    var thresholds = new
    {
        perFeed = new
        {
            robustZ = cfg.Detect.RobustZThreshold,
            recoveryZ = cfg.Detect.RecoveryZThreshold,
            persistSamples = cfg.Detect.PersistSamples,
            minSamples = cfg.Detect.MinSamples,
            maxSampleAgeSeconds = (int)cfg.Detect.MaxSampleAge.TotalSeconds
        },
        global = new
        {
            robustZ = cfg.Detect.GlobalRobustZThreshold,
            minSamples = cfg.Detect.GlobalMinSamples,
            newFeedMinListeners = cfg.Detect.NewFeedMinListeners,
            bucketSize = 5
        }
    };

    return Results.Ok(new
    {
        feedId,
        name = feedName,
        url = feedUrl,
        nowUtc = now,
        window = new
        {
            perFeedFromUtc = fromUtc,
            globalFromUtc = globalFromUtc
        },

        current = new
        {
            tsUtc = current.tsUtc,
            listeners = current.listeners,
            rank = current.rank,
            sampleAgeSeconds = (int)sampleAge.TotalSeconds
        },

        baseline = new
        {
            used = baselineUsed,
            perFeedAllowed,
            hasPerFeedBaseline,
            median,
            mad,
            currentRobustZ = currentZ,
            recentRobustZ = recentZ, // array, null if not available
            bucketUsed,
            inferredBucket,
            bucketStatsUsed
        },

        thresholds,

        decision = new
        {
            isSpikeNow,
            isRecovered
        }
    });
});


app.Run();

static int Clamp(int v, int min, int max)
{
    return v < min ? min : (v > max ? max : v);
}

static int ClampBucket(int bucket) => bucket < 0 ? 0 : (bucket > 4 ? 4 : bucket);

static int InferBucketFromListeners(
    int listeners,
    IReadOnlyDictionary<int, GlobalBucketStats> stats)
{
    var bestBucket = 4;
    var bestDistance = double.MaxValue;

    foreach (var (bucket, s) in stats)
    {
        if (s.SampleCount == 0) continue;
        var d = Math.Abs(listeners - s.Median);
        if (d < bestDistance)
        {
            bestDistance = d;
            bestBucket = bucket;
        }
    }

    return ClampBucket(bestBucket);
}

static Dictionary<int, GlobalBucketStats> ComputeGlobalBucketStats(
    IEnumerable<(int rank, int listeners)> samples,
    int bucketSize)
{
    if (bucketSize <= 0) throw new ArgumentOutOfRangeException(nameof(bucketSize));

    var byBucket = new Dictionary<int, List<double>>();

    foreach (var s in samples)
    {
        var rank = s.rank;
        if (rank < 1) continue;
        var bucket = (rank - 1) / bucketSize;

        if (!byBucket.TryGetValue(bucket, out var list))
        {
            list = new List<double>(256);
            byBucket[bucket] = list;
        }

        list.Add(s.listeners);
    }

    var result = new Dictionary<int, GlobalBucketStats>(byBucket.Count);

    foreach (var (bucket, list) in byBucket)
    {
        if (list.Count == 0)
        {
            result[bucket] = new GlobalBucketStats(bucket, 0, 0, 0);
            continue;
        }

        var values = list.ToArray();
        var median = Median(values);

        var dev = new double[values.Length];
        for (var i = 0; i < values.Length; i++)
            dev[i] = Math.Abs(values[i] - median);

        var mad = Median(dev);

        result[bucket] = new GlobalBucketStats(bucket, median, mad, values.Length);
    }

    return result;
}

static double RobustZ(double x, double median, double mad) => 0.6745 * (x - median) / mad;

static double Median(double[] xs)
{
    if (xs.Length == 0) return 0;
    var a = (double[])xs.Clone();
    Array.Sort(a);
    var mid = a.Length / 2;
    return (a.Length % 2 == 1) ? a[mid] : (a[mid - 1] + a[mid]) / 2.0;
}

static double MAD(double[] xs, double median)
{
    var dev = xs.Select(x => Math.Abs(x - median)).ToArray();
    return Median(dev);
}

readonly record struct GlobalBucketStats(int Bucket, double Median, double Mad, int SampleCount);
