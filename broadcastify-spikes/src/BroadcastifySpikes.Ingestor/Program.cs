using BroadcastifySpikes.Core;
using HtmlAgilityPack;
using Npgsql;
using StackExchange.Redis;

namespace BroadcastifySpikes.Ingestor;

internal static class Program
{
    private static readonly HttpClient Http = new(new HttpClientHandler
    {
        AutomaticDecompression = System.Net.DecompressionMethods.All
    });

    public static async Task Main(string[] args)
    {
        var cfg = AppConfig.FromEnvironment();

        var topUrl =
            Environment.GetEnvironmentVariable("TOP_FEEDS_URL")
            ?? "https://m.broadcastify.com/listen/top";

        var pollSeconds =
            int.TryParse(Environment.GetEnvironmentVariable("INGEST_POLL_SECONDS"), out var ps) ? ps : 60;

        // If a feed hasn't been seen in this long, treat it as "reappeared"
        var reappearHours =
            int.TryParse(Environment.GetEnvironmentVariable("FEED_REAPPEAR_HOURS"), out var rh) ? rh : 24;

        Console.WriteLine($"[ingestor] topUrl={topUrl}");
        Console.WriteLine($"[ingestor] pollSeconds={pollSeconds}");
        Console.WriteLine($"[ingestor] reappearHours={reappearHours}");

        var store = new PostgresStore(cfg.Db.ConnectionString);
        await store.InitializeAsync(CancellationToken.None);

        var mux = await ConnectionMultiplexer.ConnectAsync(cfg.Redis.ConnectionString);
        var queue = new RedisQueue(mux);
        await queue.EnsureConsumerGroupAsync();

        while (true)
        {
            var nowUtc = DateTimeOffset.UtcNow;

            try
            {
                var runId = Guid.NewGuid();

                await InsertIngestRunStartAsync(cfg.Db.ConnectionString, runId, nowUtc, CancellationToken.None);

                var html = await Http.GetStringAsync(topUrl);
                var rows = ParseTopFeeds(html);

                var pulledCount = 0;

                foreach (var r in rows)
                {
                    pulledCount++;

                    // Upsert feed metadata
                    await store.UpsertFeedAsync(new FeedDefinition(r.FeedId, r.Name, r.Url), CancellationToken.None);

                    // Determine new vs reappeared based on last seen
                    var lastSeen = await store.GetLastSeenUtcAsync(r.FeedId, CancellationToken.None);
                    var isNew = lastSeen is null;
                    var isReappeared = lastSeen is not null &&
                                       (nowUtc - lastSeen.Value) >= TimeSpan.FromHours(reappearHours);

                    // Mark seen timestamps
                    await store.SetSeenTimestampsAsync(
                        r.FeedId,
                        nowUtc,
                        setFirstSeenIfNull: true,
                        CancellationToken.None);

                    // Insert sample (rank included)
                    await store.InsertSampleAsync(
                        new FeedSample(r.FeedId, nowUtc, r.Listeners, r.Rank),
                        CancellationToken.None);

                    // Record this run's pulled rows
                    await UpsertIngestRunItemAsync(cfg.Db.ConnectionString, runId, r.FeedId, nowUtc, r.Listeners, r.Rank, CancellationToken.None);

                    // Emit feed_seen events (reason is a string, no enum dependency)
                    if (isNew || isReappeared)
                    {
                        var reason = isNew ? "new" : "reappeared";

                        // FeedSeenEvent must accept (feedId, name, url, tsUtc, reason)
                        await queue.EnqueueAsync(
                            EventTypes.FeedSeen,
                            new FeedSeenEvent(r.FeedId, r.Name, r.Url, nowUtc, reason));
                    }
                }

                await UpdateIngestRunCompleteAsync(cfg.Db.ConnectionString, runId, DateTimeOffset.UtcNow, pulledCount, CancellationToken.None);

                Console.WriteLine($"[ingestor] pulled={pulledCount} at {nowUtc:O}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ingestor] error: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(pollSeconds));
        }
    }

    private static List<ParsedFeedRow> ParseTopFeeds(string html)
    {
        var doc = new HtmlDocument();
        doc.LoadHtml(html);

        var trs = doc.DocumentNode.SelectNodes("//table//tbody/tr") ?? new HtmlNodeCollection(null);
        var result = new List<ParsedFeedRow>(trs.Count);

        var rank = 0;

        foreach (var tr in trs)
        {
            var a = tr.SelectSingleNode(".//a[contains(@href,'/listen/feed/')]");
            if (a is null) continue;

            var href = a.GetAttributeValue("href", "").Trim();
            var feedId = ExtractFeedId(href);
            if (string.IsNullOrWhiteSpace(feedId)) continue;

            var name = HtmlEntity.DeEntitize(a.InnerText).Trim();
            var url = $"https://m.broadcastify.com{href}";

            // increment rank only for valid feed rows
            rank++;

            // Parse listeners ONLY from the "Status" column to avoid capturing
            // other numbers like "(100 minutes ago)" from alert blocks.
            var statusTd = tr.SelectSingleNode("./td[last()]");
            var badge = statusTd?.SelectSingleNode(".//span[contains(@class,'badge')]");

            var statusText = badge is not null
                ? HtmlEntity.DeEntitize(badge.InnerText).Trim() // e.g. "141 Listeners"
                : HtmlEntity.DeEntitize(statusTd?.InnerText ?? "").Trim();

            var listeners = ExtractFirstInt(statusText);

            result.Add(new ParsedFeedRow(feedId, name, url, listeners, rank));
        }

        return result;
    }

    private static string ExtractFeedId(string href)
    {
        if (string.IsNullOrWhiteSpace(href)) return "";

        var parts = href.Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (var i = parts.Length - 1; i >= 0; i--)
        {
            if (parts[i].All(char.IsDigit))
                return parts[i];
        }

        return "";
    }

    private static int ExtractFirstInt(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return 0;

        var i = 0;
        while (i < s.Length && !char.IsDigit(s[i])) i++;
        if (i == s.Length) return 0;

        var j = i;
        while (j < s.Length && char.IsDigit(s[j])) j++;

        return int.TryParse(s.Substring(i, j - i), out var n) ? n : 0;
    }

    private static async Task InsertIngestRunStartAsync(string cs, Guid runId, DateTimeOffset startedAtUtc, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(token);

        const string sql = @"
INSERT INTO ingest_runs(run_id, started_at_utc, completed_at_utc, pulled_count)
VALUES (@rid, @started, NULL, 0);";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@rid", runId);
        cmd.Parameters.AddWithValue("@started", startedAtUtc.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(token);
    }

    private static async Task UpsertIngestRunItemAsync(string cs, Guid runId, string feedId, DateTimeOffset tsUtc, int listeners, int rank, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(token);

        const string sql = @"
INSERT INTO ingest_run_items(run_id, feed_id, ts_utc, listeners, rank)
VALUES (@rid, @fid, @ts, @l, @r)
ON CONFLICT (run_id, feed_id) DO UPDATE
SET ts_utc = excluded.ts_utc,
    listeners = excluded.listeners,
    rank = excluded.rank;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@rid", runId);
        cmd.Parameters.AddWithValue("@fid", feedId);
        cmd.Parameters.AddWithValue("@ts", tsUtc.UtcDateTime);
        cmd.Parameters.AddWithValue("@l", listeners);
        cmd.Parameters.AddWithValue("@r", rank);
        await cmd.ExecuteNonQueryAsync(token);
    }

    private static async Task UpdateIngestRunCompleteAsync(string cs, Guid runId, DateTimeOffset completedAtUtc, int pulledCount, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(token);

        const string sql = @"
UPDATE ingest_runs
SET completed_at_utc = @done,
    pulled_count = @cnt
WHERE run_id = @rid;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@rid", runId);
        cmd.Parameters.AddWithValue("@done", completedAtUtc.UtcDateTime);
        cmd.Parameters.AddWithValue("@cnt", pulledCount);
        await cmd.ExecuteNonQueryAsync(token);
    }

    private readonly record struct ParsedFeedRow(string FeedId, string Name, string Url, int Listeners, int Rank);
}
