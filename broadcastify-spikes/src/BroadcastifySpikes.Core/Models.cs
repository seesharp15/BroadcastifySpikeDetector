namespace BroadcastifySpikes.Core;

public sealed record FeedDefinition(string FeedId, string Name, string Url);

public sealed record FeedSample(string FeedId, DateTimeOffset TimestampUtc, int ListenerCount);

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

