namespace BroadcastifySpikes.Core;

public interface IAlertSink
{
    Task SendAsync(SpikeEvent e, CancellationToken token);
}
