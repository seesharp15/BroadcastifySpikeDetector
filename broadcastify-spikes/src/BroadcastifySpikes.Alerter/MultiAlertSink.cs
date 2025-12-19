using BroadcastifySpikes.Core;

namespace BroadcastifySpikes.Alerter;

public sealed class MultiAlertSink : IAlertSink
{
    private readonly IReadOnlyList<IAlertSink> _sinks;

    public MultiAlertSink(params IAlertSink[] sinks)
    {
        this._sinks = sinks;
    }

    public async Task SendAsync(SpikeEvent e, CancellationToken token)
    {
        foreach (var s in this._sinks)
        {
            try { await s.SendAsync(e, token); }
            catch (Exception ex) { Console.WriteLine($"[alerter] sink error: {ex.Message}"); }
        }
    }
}
