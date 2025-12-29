namespace BroadcastifySpikes.Core;

public sealed record FeedDefinition(string FeedId, string Name, string Url);

// Rank is nullable for backward compatibility and for any inserts that don't provide it yet.
public sealed record FeedSample(string FeedId, DateTimeOffset TimestampUtc, int ListenerCount, int? Rank = null);

public sealed record SpikeEvent(
    string FeedId,
    string Name,
    string Url,
    DateTimeOffset TimestampUtc,
    int ListenerCount,
    double Median,
    double Mad,
    double RobustZ);

public sealed record SpikeState(string FeedId, bool IsActive, DateTimeOffset? ActivatedAtUtc);

public sealed record GlobalBucketStats(int Bucket, double Median, double Mad, long SampleCount);
public sealed record GlobalBucketSample(int Bucket, double ListenerCount);
public sealed record GlobalRankSample(int Rank, double ListenerCount);
