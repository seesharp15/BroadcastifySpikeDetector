using BroadcastifySpikes.Core;

namespace BroadcastifySpikes.Alerter;
internal class SlackAlertSink : IAlertSink
{
    private readonly string? webhook;

    public SlackAlertSink(string? webhook)
    {
        this.webhook = webhook;
    }

    public Task SendAsync(SpikeEvent e, CancellationToken token)
    {
        throw new NotImplementedException();
    }
}