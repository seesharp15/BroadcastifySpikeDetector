namespace BroadcastifySpikes.Core;

public sealed class ConsoleAlertSink : IAlertSink
{
    public Task SendAsync(SpikeEvent e, CancellationToken token)
    {
        Console.WriteLine(
            $"[alerter] ALERT {e.Name} ({e.FeedId}) listeners={e.ListenerCount} rz={e.RobustZ:F2} median={e.Median:F1} mad={e.Mad:F1} url={e.Url}");
        return Task.CompletedTask;
    }
}
