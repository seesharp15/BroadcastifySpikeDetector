using StackExchange.Redis;
using System.Text.Json;

namespace BroadcastifySpikes.Core;

public sealed class RedisQueue
{
    private readonly IDatabase _db;

    public const string StreamName = "events";
    public const string ConsumerGroup = "workers";

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public RedisQueue(ConnectionMultiplexer mux)
    {
        this._db = mux.GetDatabase();
    }

    public async Task EnsureConsumerGroupAsync()
    {
        // Creates stream + group if missing
        try
        {
            await this._db.ExecuteAsync("XGROUP", "CREATE", StreamName, ConsumerGroup, "0", "MKSTREAM");
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            // already exists
        }
    }

    public async Task EnqueueAsync<T>(string eventType, T payload)
    {
        var json = JsonSerializer.Serialize(payload, JsonOpts);

        await this._db.StreamAddAsync(StreamName, new NameValueEntry[]
        {
            new("event_type", eventType),
            new("payload", json),
            new("enqueued_at_utc", DateTimeOffset.UtcNow.UtcDateTime.ToString("O"))
        });
    }

    public async Task<IReadOnlyList<(RedisValue Id, string EventType, string PayloadJson)>> ReadAsync(
        string consumerName,
        int count,
        int blockMs)
    {
        var entries = await this._db.StreamReadGroupAsync(StreamName, ConsumerGroup, consumerName, ">", count);
        if (entries.Length == 0)
        {
            await Task.Delay(blockMs);
        }
        var list = new List<(RedisValue, string, string)>();
        foreach (var e in entries)
        {
            var eventType = e.Values.FirstOrDefault(v => v.Name == "event_type").Value;
            var payload = e.Values.FirstOrDefault(v => v.Name == "payload").Value;

            if (eventType.IsNullOrEmpty || payload.IsNullOrEmpty)
                continue;

            list.Add((e.Id, (string)eventType!, (string)payload!));
        }

        return list;
    }

    public Task AckAsync(RedisValue id)
    {
        return this._db.StreamAcknowledgeAsync(StreamName, ConsumerGroup, id);
    }

    public static T? DeserializePayload<T>(string json)
    {
        return JsonSerializer.Deserialize<T>(json, JsonOpts);
    }
}