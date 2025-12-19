using BroadcastifySpikes.Core;

var cfg = AppConfig.FromEnvironment();

var retentionDays = int.TryParse(Environment.GetEnvironmentVariable("RETENTION_DAYS"), out var rd) ? rd : 14;
var pollSeconds = int.TryParse(Environment.GetEnvironmentVariable("CLEANUP_POLL_SECONDS"), out var ps) ? ps : 3600;

Console.WriteLine($"[cleanup] starting. retentionDays={retentionDays} pollSeconds={pollSeconds}");

var store = new PostgresStore(cfg.Db.ConnectionString);
await store.InitializeAsync(CancellationToken.None);

while (true)
{
    try
    {
        var cutoff = DateTimeOffset.UtcNow.AddDays(-retentionDays);
        var deleted = await store.DeleteSamplesOlderThanAsync(cutoff, CancellationToken.None);
        Console.WriteLine($"[cleanup] deleted {deleted} samples older than {cutoff:O}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[cleanup] error: {ex.Message}");
    }

    await Task.Delay(TimeSpan.FromSeconds(pollSeconds));
}
