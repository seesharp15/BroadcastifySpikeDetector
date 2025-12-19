using BroadcastifySpikes.Core;
using StackExchange.Redis;

var cfg = AppConfig.FromEnvironment();
Console.WriteLine($"[detector] starting. db={cfg.Db.Host}:{cfg.Db.Port}/{cfg.Db.Database} redis={cfg.Redis.Host}:{cfg.Redis.Port}");

var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

var mux = await ConnectionMultiplexer.ConnectAsync(cfg.Redis.ConnectionString);
var queue = new RedisQueue(mux);
await queue.EnsureConsumerGroupAsync();

while (true)
{
    var fromUtc = DateTimeOffset.UtcNow - cfg.Detect.LookbackWindow;

    try
    {
        var feeds = await store.GetFeedsAsync(CancellationToken.None);
        foreach (var feed in feeds)
        {
            var samples = await store.GetSamplesAsync(feed.FeedId, fromUtc, CancellationToken.None);
            if (samples.Count < cfg.Detect.MinSamples) continue;
            if (samples.Count < cfg.Detect.PersistSamples + 2) continue;

            var n = cfg.Detect.PersistSamples;
            var recent = samples.Skip(samples.Count - n).ToArray();
            var baseline = samples.Take(samples.Count - n).Select(s => (double)s.ListenerCount).ToArray();

            if (baseline.Length < cfg.Detect.MinSamples) continue;

            var median = Median(baseline);
            var mad = MAD(baseline, median);
            if (mad <= 1e-9) continue;

            var recentZ = recent.Select(s => RobustZ(s.ListenerCount, median, mad)).ToArray();
            var isSpikeNow = recentZ.All(z => z >= cfg.Detect.RobustZThreshold);

            var current = samples[^1];
            var currentZ = RobustZ(current.ListenerCount, median, mad);
            var isRecovered = currentZ <= cfg.Detect.RecoveryZThreshold;

            var state = await store.GetSpikeStateAsync(feed.FeedId, CancellationToken.None);

            if (!state.IsActive)
            {
                if (isSpikeNow)
                {
                    var e = new SpikeEvent(
                        feed.FeedId,
                        feed.Name,
                        feed.Url,
                        current.TimestampUtc,
                        current.ListenerCount,
                        median,
                        mad,
                        currentZ);

                    await queue.EnqueueAsync(EventTypes.Spike, e);
                    await store.SetSpikeStateAsync(new SpikeState(feed.FeedId, true, DateTimeOffset.UtcNow), CancellationToken.None);

                    Console.WriteLine($"[detector] spike START {feed.FeedId} rz={currentZ:F2} median={median:F1} mad={mad:F1} listeners={current.ListenerCount}");
                }
            }
            else
            {
                if (isRecovered)
                {
                    await store.SetSpikeStateAsync(new SpikeState(feed.FeedId, false, null), CancellationToken.None);
                    Console.WriteLine($"[detector] spike END {feed.FeedId} rz={currentZ:F2}");
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[detector] error: {ex.Message}");
    }

    await Task.Delay(cfg.Detect.PollInterval);
}

static double RobustZ(double x, double median, double mad)
{
    return 0.6745 * (x - median) / mad;
}

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
