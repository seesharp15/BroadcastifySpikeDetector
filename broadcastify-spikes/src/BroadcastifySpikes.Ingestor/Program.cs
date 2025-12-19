using BroadcastifySpikes.Core;
using HtmlAgilityPack;
using StackExchange.Redis;
using System.Net.Http.Headers;

var cfg = AppConfig.FromEnvironment();
Console.WriteLine($"[ingestor] starting. db={cfg.Db.Host}:{cfg.Db.Port}/{cfg.Db.Database} redis={cfg.Redis.Host}:{cfg.Redis.Port}");

var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

var mux = await ConnectionMultiplexer.ConnectAsync(cfg.Redis.ConnectionString);
var queue = new RedisQueue(mux);
await queue.EnsureConsumerGroupAsync();

var reappearDays = int.TryParse(Environment.GetEnvironmentVariable("REAPPEAR_DAYS"), out var rd) ? rd : 7;
var reappearThreshold = TimeSpan.FromDays(reappearDays);

using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
http.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("BroadcastifySpikes", "1.0"));

while (true)
{
    var nowUtc = DateTimeOffset.UtcNow;

    try
    {
        var html = await http.GetStringAsync(cfg.Ingest.TopUrl);
        var doc = new HtmlDocument();
        doc.LoadHtml(html);

        var links = doc.DocumentNode.SelectNodes("//a[contains(@href, '/feed/')]");
        if (links is null || links.Count == 0)
        {
            Console.WriteLine("[ingestor] no /feed/ links found (page changed?)");
            await Task.Delay(cfg.Ingest.PollInterval);
            continue;
        }

        var seenThisRun = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var stored = 0;
        var emitted = 0;

        foreach (var a in links)
        {
            var href = a.GetAttributeValue("href", "").Trim();
            if (string.IsNullOrWhiteSpace(href)) continue;

            var feedId = ExtractFeedId(href);
            if (string.IsNullOrWhiteSpace(feedId)) continue;

            if (!seenThisRun.Add(feedId)) continue;

            var name = HtmlEntity.DeEntitize(a.InnerText).Trim();
            if (string.IsNullOrWhiteSpace(name)) name = $"Feed {feedId}";

            var url = href.StartsWith("http", StringComparison.OrdinalIgnoreCase)
                ? href
                : $"https://m.broadcastify.com{href}";

            var listeners = ExtractListenersFromRow(a);

            // Determine if this is "new to us" or "reappeared"
            var existing = await store.GetFeedAsync(feedId, CancellationToken.None);
            var lastSeen = existing is null ? null : await store.GetLastSeenUtcAsync(feedId, CancellationToken.None);

            var isNew = existing is null;
            var isReappeared = !isNew && lastSeen is not null && (nowUtc - lastSeen.Value) >= reappearThreshold;

            await store.UpsertFeedAsync(new FeedDefinition(feedId, name, url), CancellationToken.None);
            await store.SetSeenTimestampsAsync(feedId, nowUtc, setFirstSeenIfNull: true, CancellationToken.None);
            await store.InsertSampleAsync(new FeedSample(feedId, nowUtc, listeners), CancellationToken.None);

            stored++;

            if (isNew || isReappeared)
            {
                var reason = isNew ? "new" : "reappeared";
                var evt = new FeedSeenEvent(feedId, name, url, nowUtc, reason);

                await queue.EnqueueAsync(EventTypes.FeedSeen, evt);

                emitted++;
                Console.WriteLine($"[ingestor] FEED_SEEN ({reason}) {feedId} {name}");
            }
        }

        Console.WriteLine($"[ingestor] stored {stored} samples, emitted {emitted} feed_seen events @ {nowUtc:O}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[ingestor] error: {ex.Message}");
    }

    await Task.Delay(cfg.Ingest.PollInterval);
}

static string? ExtractFeedId(string href)
{
    var idx = href.IndexOf("/feed/", StringComparison.OrdinalIgnoreCase);
    if (idx < 0) return null;

    var tail = href[(idx + "/feed/".Length)..];
    var id = new string(tail.TakeWhile(char.IsDigit).ToArray());
    return string.IsNullOrWhiteSpace(id) ? null : id;
}

static int ExtractListenersFromRow(HtmlNode a)
{
    var tr = a.Ancestors("tr").FirstOrDefault();
    if (tr is null) return 0;

    // status cell is the other <td> in the row; it contains something like:
    // <span class="badge badge-success">141 Listeners</span>
    var statusText = HtmlEntity.DeEntitize(tr.InnerText).Trim();

    // More targeted: look for the badge span first (less noisy than full row text)
    var badge = tr.SelectSingleNode(".//span[contains(@class,'badge')]");
    if (badge is not null)
        statusText = HtmlEntity.DeEntitize(badge.InnerText).Trim();

    // Extract leading integer anywhere in the string (handles "141 Listeners")
    var digits = new string(statusText.TakeWhile(c => !char.IsDigit(c)).ToArray());
    // That line is wrong: we want the digits, not the prefix. We'll do proper scan instead.

    var numberStr = ExtractFirstInteger(statusText);
    return int.TryParse(numberStr, out var n) ? n : 0;
}

static string ExtractFirstInteger(string s)
{
    // Finds the first contiguous run of digits anywhere in the string
    var i = 0;
    while (i < s.Length && !char.IsDigit(s[i])) i++;
    if (i == s.Length) return "";

    var j = i;
    while (j < s.Length && char.IsDigit(s[j])) j++;

    return s.Substring(i, j - i);
}
