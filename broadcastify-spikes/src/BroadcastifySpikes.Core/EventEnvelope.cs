namespace BroadcastifySpikes.Core;

public static class EventTypes
{
    public const string Spike = "spike";
    public const string FeedSeen = "feed_seen"; // includes "new" and "reappeared"
}

public sealed record FeedSeenEvent(
    string FeedId,
    string Name,
    string Url,
    DateTimeOffset TimestampUtc,
    string Reason // "new" | "reappeared"
);

public sealed record EventEnvelope(
    string EventType,
    string PayloadJson,
    DateTimeOffset EnqueuedAtUtc
);
