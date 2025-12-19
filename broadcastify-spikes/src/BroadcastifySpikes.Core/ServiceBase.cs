namespace BroadcastifySpikes.Core;

public abstract class ServiceBase
{
    private readonly CancellationTokenSource _cts = new();
    private Task? _runner;

    public string Name { get; }

    protected ServiceBase(string name)
    {
        this.Name = name;
    }

    public void Start()
    {
        this._runner = Task.Run(() => this.RunAsync(this._cts.Token));
    }

    public async Task StopAsync()
    {
        this._cts.Cancel();
        if (this._runner is not null)
        {
            try { await this._runner.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        this._cts.Dispose();
    }

    private async Task RunAsync(CancellationToken token)
    {
        await this.OnStartAsync(token).ConfigureAwait(false);

        while (!token.IsCancellationRequested)
        {
            await this.ExecuteOnceAsync(token).ConfigureAwait(false);

            var delay = this.GetDelay();
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, token).ConfigureAwait(false);
        }

        await this.OnStopAsync(CancellationToken.None).ConfigureAwait(false);
    }

    protected virtual Task OnStartAsync(CancellationToken token)
    {
        return Task.CompletedTask;
    }

    protected virtual Task OnStopAsync(CancellationToken token)
    {
        return Task.CompletedTask;
    }

    protected abstract Task ExecuteOnceAsync(CancellationToken token);
    protected abstract TimeSpan GetDelay();
}
