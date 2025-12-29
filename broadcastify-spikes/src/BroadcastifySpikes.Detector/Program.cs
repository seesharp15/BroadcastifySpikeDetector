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
    try
    {
        var now = DateTimeOffset.UtcNow;

        // Per-feed window (e.g. 3 days)
        var fromUtc = now - cfg.Detect.LookbackWindow;

        // Global window (e.g. 14 days)
        var globalFromUtc = now - cfg.Detect.GlobalLookbackWindow;

        // Pull global samples ONCE and compute bucket stats ONCE per loop.
        // This assumes you have a store method that returns *all buckets* in the lookback window.
        // If your store method currently takes a bucket param, replace this with 5 calls (bucket 0..4)
        // and combine the results before computing stats.
        var globalSamples = await store.GetGlobalRankSamplesAsync(globalFromUtc, CancellationToken.None);
        var globalBucketStats = ComputeGlobalBucketStats(globalSamples, bucketSize: 5);

        var feeds = await store.GetFeedsAsync(CancellationToken.None);
        foreach (var feed in feeds)
        {
            var samples = await store.GetSamplesAsync(feed.FeedId, fromUtc, CancellationToken.None);
            if (samples.Count == 0) continue;

            var current = samples[^1];

            // If our newest sample is stale, don't trust per-feed logic
            var sampleAge = now - current.TimestampUtc;
            var perFeedAllowed = sampleAge <= cfg.Detect.MaxSampleAge;

            // Per-feed baseline calculation
            var hasPerFeedBaseline = false;
            double median = 0, mad = 0, currentZ = 0;

            if (perFeedAllowed &&
                samples.Count >= cfg.Detect.MinSamples &&
                samples.Count >= cfg.Detect.PersistSamples + 2)
            {
                var n = cfg.Detect.PersistSamples;
                var baseline = samples.Take(samples.Count - n)
                                      .Select(s => (double)s.ListenerCount)
                                      .ToArray();

                if (baseline.Length >= cfg.Detect.MinSamples)
                {
                    median = Median(baseline);
                    mad = MAD(baseline, median);
                    if (mad > 1e-9)
                    {
                        currentZ = RobustZ(current.ListenerCount, median, mad);
                        hasPerFeedBaseline = true;
                    }
                }
            }

            // Determine spike using per-feed baseline if possible
            var isSpikeNow = false;
            var isRecovered = false;

            if (hasPerFeedBaseline)
            {
                var n = cfg.Detect.PersistSamples;
                var recent = samples.Skip(samples.Count - n).ToArray();
                var recentZ = recent.Select(s => RobustZ(s.ListenerCount, median, mad)).ToArray();

                isSpikeNow = recentZ.All(z => z >= cfg.Detect.RobustZThreshold);
                isRecovered = currentZ <= cfg.Detect.RecoveryZThreshold;
            }
            else
            {

                // later for a given feed/sample:
                var rank = current.Rank; // if you have it
                                         //  var bucket = (Math.Clamp(rank ?? 25, 1, 25) - 1) / 5;

                // Fallback: global rank-bucket baseline.
                // If Rank is null, infer a bucket based on closest global bucket median.
                var bucket = current.Rank is not null
                    ? (Math.Clamp(rank ?? 25, 1, 25) - 1) / 5
                    : InferBucketFromListeners(current.ListenerCount, globalBucketStats);

                if (globalBucketStats.TryGetValue(bucket, out var gs) &&
                    gs.SampleCount >= cfg.Detect.GlobalMinSamples &&
                    gs.Mad > 1e-9)
                {
                    median = gs.Median;
                    mad = gs.Mad;
                    currentZ = RobustZ(current.ListenerCount, median, mad);

                    // "global spike" definition (handles new/reappeared feeds with no per-feed baseline)
                    isSpikeNow =
                        currentZ >= cfg.Detect.GlobalRobustZThreshold &&
                        current.ListenerCount >= cfg.Detect.NewFeedMinListeners;

                    // Recovered when it falls back near median
                    isRecovered = currentZ <= cfg.Detect.RecoveryZThreshold;
                }
                else
                {
                    // If even global stats are missing, last-resort: hard threshold
                    isSpikeNow = current.ListenerCount >= cfg.Detect.NewFeedMinListeners;
                    isRecovered = current.ListenerCount < cfg.Detect.NewFeedMinListeners;
                }
            }

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
                    await store.SetSpikeStateAsync(new SpikeState(feed.FeedId, true, now), CancellationToken.None);

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

static int ClampBucket(int bucket)
{
    if (bucket < 0) return 0;
    return bucket > 4 ? 4 : bucket;
}

static int InferBucketFromListeners(
    int listeners,
    IReadOnlyDictionary<int, GlobalBucketStats> stats)
{
    var bestBucket = 4; // default to lowest
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
    IEnumerable<GlobalRankSample> samples,
    int bucketSize)
{
    if (bucketSize <= 0) throw new ArgumentOutOfRangeException(nameof(bucketSize));

    // bucketIndex: 0..N-1 (for top 25 with bucketSize=5 => 0..4)
    var byBucket = new Dictionary<int, List<double>>();

    foreach (var s in samples)
    {
        var rank = s.Rank;
        if (rank < 1) continue; // defensive
        var bucket = (rank - 1) / bucketSize;

        if (!byBucket.TryGetValue(bucket, out var list))
        {
            list = new List<double>(256);
            byBucket[bucket] = list;
        }

        list.Add(s.ListenerCount);
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
