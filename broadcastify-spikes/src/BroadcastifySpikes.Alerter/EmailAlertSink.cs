using BroadcastifySpikes.Core;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;

namespace BroadcastifySpikes.Alerter;

public sealed class EmailAlertSink : IAlertSink
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _user;
    private readonly string _password;
    private readonly string _from;
    private readonly string _to;
    private readonly bool _tls;

    public EmailAlertSink(string host, int port, string user, string password, string from, string to, bool tls)
    {
        this._host = host;
        this._port = port;
        this._user = user;
        this._password = password;
        this._from = from;
        this._to = to;
        this._tls = tls;
    }

    public async Task SendAsync(SpikeEvent e, CancellationToken token)
    {
        var msg = new MimeMessage();
        msg.From.Add(MailboxAddress.Parse(this._from));
        msg.To.Add(MailboxAddress.Parse(this._to));
        msg.Subject = $"Broadcastify spike: {e.Name} ({e.FeedId})";

        msg.Body = new TextPart("plain")
        {
            Text =
                $"Feed: {e.Name}\n" +
                $"FeedId: {e.FeedId}\n" +
                $"Listeners: {e.ListenerCount}\n" +
                $"RobustZ: {e.RobustZ:F2}\n" +
                $"Median: {e.Median:F1}  MAD: {e.Mad:F1}\n" +
                $"URL: {e.Url}\n" +
                $"Time (UTC): {e.TimestampUtc:O}\n"
        };

        using var client = new SmtpClient();
        var sec = this._tls ? SecureSocketOptions.StartTls : SecureSocketOptions.None;

        await client.ConnectAsync(this._host, this._port, sec, token);

        if (!string.IsNullOrWhiteSpace(this._user))
            await client.AuthenticateAsync(this._user, this._password, token);

        await client.SendAsync(msg, token);
        await client.DisconnectAsync(true, token);
    }
}
