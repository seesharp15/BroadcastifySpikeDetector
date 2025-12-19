using BroadcastifySpikes.Core;
using Npgsql;
using StackExchange.Redis;

namespace BroadcastifySpikes.Alerter;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        var cfg = AppConfig.FromEnvironment();

        var suppressHours =
            int.TryParse(Environment.GetEnvironmentVariable("ALERT_SUPPRESS_HOURS"), out var sh) ? sh : 8;

        var suppressWindow = TimeSpan.FromHours(suppressHours);

        var consumerName = $"alerter-{Environment.MachineName}-{Guid.NewGuid():N}";
        if (consumerName.Length > 32)
            consumerName = consumerName.Substring(0, 32);

        Console.WriteLine($"[alerter] starting consumer={consumerName}");
        Console.WriteLine($"[alerter] suppressWindow={suppressWindow}");

        var store = new PostgresStore(cfg.Db.ConnectionString);
        await store.InitializeAsync(CancellationToken.None);

        var mux = await ConnectionMultiplexer.ConnectAsync(cfg.Redis.ConnectionString);
        var queue = new RedisQueue(mux);
        await queue.EnsureConsumerGroupAsync();

        // sinks
        var sinks = new List<IAlertSink> { new ConsoleAlertSink() };

        var slackWebhook = Environment.GetEnvironmentVariable("SLACK_WEBHOOK_URL");
        if (!string.IsNullOrWhiteSpace(slackWebhook))
        {
            sinks.Add(new SlackAlertSink(slackWebhook));
            Console.WriteLine("[alerter] Slack sink enabled");
        }

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
            {
                sinks.Add(new EmailAlertSink(smtpHost, smtpPort, smtpUser, smtpPw, smtpFrom, smtpTo, tls));
                Console.WriteLine("[alerter] Email sink enabled");
            }
        }

        IAlertSink sink = new MultiAlertSink(sinks.ToArray());

        while (true)
        {
            try
            {
                var batch = await queue.ReadAsync(consumerName, count: 25, blockMs: 5000);
                if (batch.Count == 0)
                    continue;

                foreach (var (id, eventType, payloadJson) in batch)
                {
                    try
                    {
                        var now = DateTimeOffset.UtcNow;

                        if (eventType == EventTypes.Spike)
                        {
                            var e = RedisQueue.DeserializePayload<SpikeEvent>(payloadJson);
                            if (e is null)
                            {
                                await queue.AckAsync(id);
                                continue;
                            }

                            // Per-feed suppression
                            var last = await store.GetLastAlertUtcAsync(e.FeedId, CancellationToken.None);
                            if (last is not null && (now - last.Value) < suppressWindow)
                            {
                                Console.WriteLine($"[alerter] suppressed spike {e.FeedId} last={last:O}");
                                await queue.AckAsync(id);
                                continue;
                            }

                            await sink.SendAsync(e, CancellationToken.None);
                            await store.SetLastAlertUtcAsync(e.FeedId, now, CancellationToken.None);

                            // Persist robust stats in alert history
                            var msg =
                                $"Spike robustZ={e.RobustZ:F2} listeners={e.ListenerCount} " +
                                $"median={e.Median:F2} mad={e.Mad:F2} " +
                                $"name={e.Name}";

                            await InsertAlertHistoryAsync(
                                cfg.Db.ConnectionString,
                                now,
                                e.FeedId,
                                "spike",
                                msg,
                                CancellationToken.None);

                            await queue.AckAsync(id);
                            continue;
                        }

                        if (eventType == EventTypes.FeedSeen)
                        {
                            var fs = RedisQueue.DeserializePayload<FeedSeenEvent>(payloadJson);
                            if (fs is null)
                            {
                                await queue.AckAsync(id);
                                continue;
                            }

                            var last = await store.GetLastAlertUtcAsync(fs.FeedId, CancellationToken.None);
                            if (last is not null && (now - last.Value) < suppressWindow)
                            {
                                Console.WriteLine($"[alerter] suppressed feed_seen {fs.FeedId} last={last:O}");
                                await queue.AckAsync(id);
                                continue;
                            }

                            Console.WriteLine($"[alerter] FEED_SEEN {fs.Name} ({fs.FeedId}) {fs.Url}");

                            await store.SetLastAlertUtcAsync(fs.FeedId, now, CancellationToken.None);

                            var feedSeenMsg = BuildFeedSeenMessage(fs);

                            await InsertAlertHistoryAsync(
                                cfg.Db.ConnectionString,
                                now,
                                fs.FeedId,
                                "feed_seen",
                                feedSeenMsg,
                                CancellationToken.None);

                            await queue.AckAsync(id);
                            continue;
                        }

                        Console.WriteLine($"[alerter] unknown event_type={eventType}");
                        await queue.AckAsync(id);
                    }
                    catch (Exception ex)
                    {
                        // ACK so a bad event doesn't poison the consumer group
                        Console.WriteLine($"[alerter] event handling error ({eventType}): {ex.Message}");
                        await queue.AckAsync(id);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[alerter] loop error: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }
    }

    private static string BuildFeedSeenMessage(FeedSeenEvent fs)
    {
        // If FeedSeenEvent includes Reason, include it; otherwise basic.
        try
        {
            var prop = fs.GetType().GetProperty("Reason");
            if (prop is not null)
            {
                var v = prop.GetValue(fs)?.ToString();
                if (!string.IsNullOrWhiteSpace(v))
                    return $"{v}: {fs.Name}";
            }
        }
        catch
        {
            // ignore
        }

        return fs.Name;
    }

    private static async Task InsertAlertHistoryAsync(
        string cs,
        DateTimeOffset tsUtc,
        string feedId,
        string alertType,
        string message,
        CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync(token);

        const string sql = @"
INSERT INTO alert_history(ts_utc, feed_id, alert_type, message)
VALUES (@ts, @fid, @type, @msg);";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@ts", tsUtc.UtcDateTime);
        cmd.Parameters.AddWithValue("@fid", feedId);
        cmd.Parameters.AddWithValue("@type", alertType);
        cmd.Parameters.AddWithValue("@msg", (object?)message ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(token);
    }
}
