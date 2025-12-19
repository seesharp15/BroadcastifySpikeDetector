using BroadcastifySpikes.Alerter;
using BroadcastifySpikes.Core;
using StackExchange.Redis;

var cfg = AppConfig.FromEnvironment();
var consumer = $"alerter-{Environment.MachineName}-{Guid.NewGuid():N}".Substring(0, 32);

Console.WriteLine($"[alerter] starting consumer={consumer}");

var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

var mux = await ConnectionMultiplexer.ConnectAsync(cfg.Redis.ConnectionString);
var queue = new RedisQueue(mux);
await queue.EnsureConsumerGroupAsync();

// Build sinks from env
var sinks = new List<IAlertSink> { new ConsoleAlertSink() };

var smtpHost = Environment.GetEnvironmentVariable("SMTP_HOST");
if (!string.IsNullOrWhiteSpace(smtpHost))
{
    var smtpPort = int.TryParse(Environment.GetEnvironmentVariable("SMTP_PORT"), out var p) ? p : 587;
    var smtpUser = Environment.GetEnvironmentVariable("SMTP_USER") ?? "";
    var smtpPw = Environment.GetEnvironmentVariable("SMTP_PASSWORD") ?? "";
    var smtpFrom = Environment.GetEnvironmentVariable("SMTP_FROM") ?? "";
    var smtpTo = Environment.GetEnvironmentVariable("SMTP_TO") ?? "";
    var tls = !string.Equals(Environment.GetEnvironmentVariable("SMTP_ENABLE_TLS"), "false", StringComparison.OrdinalIgnoreCase);

    if (!string.IsNullOrWhiteSpace(smtpFrom) && !string.IsNullOrWhiteSpace(smtpTo))
        sinks.Add(new EmailAlertSink(smtpHost, smtpPort, smtpUser, smtpPw, smtpFrom, smtpTo, tls));
}

var sink = new MultiAlertSink(sinks.ToArray());

while (true)
{
    try
    {
        var batch = await queue.ReadAsync(consumer, count: 25, blockMs: 5000);
        if (batch.Count == 0) continue;

        foreach (var (id, eventType, payloadJson) in batch)
        {
            if (eventType == EventTypes.Spike)
            {
                var e = RedisQueue.DeserializePayload<SpikeEvent>(payloadJson);
                if (e is null) { await queue.AckAsync(id); continue; }

                // suppression keyed by feed_id
                var last = await store.GetLastAlertUtcAsync(e.FeedId, CancellationToken.None);
                var now = DateTimeOffset.UtcNow;

                if (last is not null && (now - last.Value) < cfg.Alert.SuppressWindow)
                {
                    Console.WriteLine($"[alerter] suppressed spike {e.FeedId} last={last:O}");
                    await queue.AckAsync(id);
                    continue;
                }

                await sink.SendAsync(e, CancellationToken.None);
                await store.SetLastAlertUtcAsync(e.FeedId, now, CancellationToken.None);
                await queue.AckAsync(id);
            }
            else if (eventType == EventTypes.FeedSeen)
            {
                var fs = RedisQueue.DeserializePayload<FeedSeenEvent>(payloadJson);
                if (fs is null) { await queue.AckAsync(id); continue; }

                // Use same suppression window or make it longer by env; using same for now
                var last = await store.GetLastAlertUtcAsync(fs.FeedId, CancellationToken.None);
                var now = DateTimeOffset.UtcNow;

                if (last is not null && (now - last.Value) < cfg.Alert.SuppressWindow)
                {
                    Console.WriteLine($"[alerter] suppressed feed_seen {fs.FeedId} last={last:O}");
                    await queue.AckAsync(id);
                    continue;
                }

                Console.WriteLine($"[alerter] FEED_SEEN ALERT ({fs.Reason}) {fs.Name} ({fs.FeedId}) {fs.Url}");
                await store.SetLastAlertUtcAsync(fs.FeedId, now, CancellationToken.None);
                await queue.AckAsync(id);
            }
            else
            {
                Console.WriteLine($"[alerter] unknown event_type={eventType}");
                await queue.AckAsync(id);
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[alerter] error: {ex.Message}");
        await Task.Delay(TimeSpan.FromSeconds(2));
    }
}