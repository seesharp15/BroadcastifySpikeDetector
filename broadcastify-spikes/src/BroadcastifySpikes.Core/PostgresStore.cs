using Npgsql;

namespace BroadcastifySpikes.Core;

public sealed class PostgresStore
{
    private readonly string _cs;

    public PostgresStore(string connectionString)
    {
        this._cs = connectionString;
    }

    public async Task InitializeAsync(CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"
CREATE TABLE IF NOT EXISTS feeds (
  feed_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
  id BIGSERIAL PRIMARY KEY,
  feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
  ts_utc TIMESTAMPTZ NOT NULL,
  listeners INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_samples_feed_ts ON samples(feed_id, ts_utc);

CREATE TABLE IF NOT EXISTS spike_state (
  feed_id TEXT PRIMARY KEY REFERENCES feeds(feed_id),
  is_active BOOLEAN NOT NULL,
  activated_at_utc TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS alert_state (
  feed_id TEXT PRIMARY KEY REFERENCES feeds(feed_id),
  last_alert_utc TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id UUID PRIMARY KEY,
  started_at_utc TIMESTAMPTZ NOT NULL,
  completed_at_utc TIMESTAMPTZ NULL,
  pulled_count INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingest_run_items (
  run_id UUID NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
  ts_utc TIMESTAMPTZ NOT NULL,
  listeners INT NOT NULL,
  PRIMARY KEY (run_id, feed_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_run_items_ts ON ingest_run_items(ts_utc);
CREATE INDEX IF NOT EXISTS idx_ingest_runs_completed ON ingest_runs(completed_at_utc);

CREATE TABLE IF NOT EXISTS alert_history (
  id BIGSERIAL PRIMARY KEY,
  ts_utc TIMESTAMPTZ NOT NULL,
  feed_id TEXT NOT NULL REFERENCES feeds(feed_id),
  alert_type TEXT NOT NULL,
  message TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_alert_history_ts ON alert_history(ts_utc);

-- Safe schema evolution for discovery/reappearance logic
ALTER TABLE feeds ADD COLUMN IF NOT EXISTS first_seen_utc TIMESTAMPTZ NULL;
ALTER TABLE feeds ADD COLUMN IF NOT EXISTS last_seen_utc  TIMESTAMPTZ NULL;

-- Safe schema evolution for global baseline using rank buckets (Top 25 position)
ALTER TABLE samples ADD COLUMN IF NOT EXISTS ""rank"" INT NULL;
ALTER TABLE ingest_run_items ADD COLUMN IF NOT EXISTS ""rank"" INT;


CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts_utc);
CREATE INDEX IF NOT EXISTS idx_samples_rank_ts ON samples(""rank"", ts_utc);
";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task UpsertFeedAsync(FeedDefinition def, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"
INSERT INTO feeds(feed_id, name, url) VALUES (@id, @name, @url)
ON CONFLICT(feed_id) DO UPDATE SET name=excluded.name, url=excluded.url;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", def.FeedId);
        cmd.Parameters.AddWithValue("@name", def.Name);
        cmd.Parameters.AddWithValue("@url", def.Url);
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task InsertSampleAsync(FeedSample sample, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        // rank is nullable; store NULL if missing
        var sql = @"INSERT INTO samples(feed_id, ts_utc, listeners, ""rank"") VALUES (@id, @ts, @l, @rank);";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", sample.FeedId);
        cmd.Parameters.AddWithValue("@ts", sample.TimestampUtc.UtcDateTime);
        cmd.Parameters.AddWithValue("@l", sample.ListenerCount);
        cmd.Parameters.AddWithValue("@rank", (object?)sample.Rank ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task<List<FeedDefinition>> GetFeedsAsync(CancellationToken token)
    {
        var list = new List<FeedDefinition>();

        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"SELECT feed_id, name, url FROM feeds;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(token);

        while (await reader.ReadAsync(token))
        {
            list.Add(new FeedDefinition(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2)));
        }

        return list;
    }

    public async Task<FeedDefinition?> GetFeedAsync(string feedId, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        const string sql = @"SELECT feed_id, name, url FROM feeds WHERE feed_id=@id;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);

        await using var r = await cmd.ExecuteReaderAsync(token);
        return !await r.ReadAsync(token) ? null : new FeedDefinition(r.GetString(0), r.GetString(1), r.GetString(2));
    }

    public async Task<DateTimeOffset?> GetLastSeenUtcAsync(string feedId, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        const string sql = @"SELECT last_seen_utc FROM feeds WHERE feed_id=@id;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);

        var result = await cmd.ExecuteScalarAsync(token);
        return result is null or DBNull ? null : new DateTimeOffset((DateTime)result, TimeSpan.Zero);
    }

    public async Task SetSeenTimestampsAsync(string feedId, DateTimeOffset nowUtc, bool setFirstSeenIfNull, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = setFirstSeenIfNull
            ? @"UPDATE feeds
                SET last_seen_utc=@now,
                    first_seen_utc=COALESCE(first_seen_utc, @now)
                WHERE feed_id=@id;"
            : @"UPDATE feeds
                SET last_seen_utc=@now
                WHERE feed_id=@id;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);
        cmd.Parameters.AddWithValue("@now", nowUtc.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task<List<FeedSample>> GetSamplesAsync(string feedId, DateTimeOffset fromUtc, CancellationToken token)
    {
        var list = new List<FeedSample>();

        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        // rank is nullable
        var sql = @"
SELECT feed_id, ts_utc, listeners, ""rank""
FROM samples
WHERE feed_id=@id AND ts_utc >= @from
ORDER BY ts_utc ASC;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);
        cmd.Parameters.AddWithValue("@from", fromUtc.UtcDateTime);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var rank = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);

            list.Add(new FeedSample(
                reader.GetString(0),
                new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero),
                reader.GetInt32(2),
                rank));
        }

        return list;
    }

    /// <summary>
    /// Fetch listener counts for samples whose rank is within [minRank..maxRank] over a time window.
    /// Use this for rank-bucketed global baselines (compute median/MAD in C#).
    /// </summary>
    public async Task<List<int>> GetListenerCountsForRankRangeAsync(int minRankInclusive, int maxRankInclusive, DateTimeOffset fromUtc, CancellationToken token)
    {
        var list = new List<int>();

        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"
SELECT listeners
FROM samples
WHERE ts_utc >= @from
  AND ""rank"" IS NOT NULL
  AND ""rank"" >= @minRank
  AND ""rank"" <= @maxRank;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@from", fromUtc.UtcDateTime);
        cmd.Parameters.AddWithValue("@minRank", minRankInclusive);
        cmd.Parameters.AddWithValue("@maxRank", maxRankInclusive);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            list.Add(reader.GetInt32(0));
        }

        return list;
    }

    public async Task<SpikeState> GetSpikeStateAsync(string feedId, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"SELECT is_active, activated_at_utc FROM spike_state WHERE feed_id=@id;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token))
            return new SpikeState(feedId, false, null);

        var isActive = reader.GetBoolean(0);
        DateTimeOffset? activated = reader.IsDBNull(1)
            ? null
            : new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero);

        return new SpikeState(feedId, isActive, activated);
    }

    public async Task SetSpikeStateAsync(SpikeState state, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"
INSERT INTO spike_state(feed_id, is_active, activated_at_utc)
VALUES (@id, @active, @ts)
ON CONFLICT(feed_id) DO UPDATE
SET is_active=excluded.is_active, activated_at_utc=excluded.activated_at_utc;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", state.FeedId);
        cmd.Parameters.AddWithValue("@active", state.IsActive);
        cmd.Parameters.AddWithValue("@ts", (object?)state.ActivatedAtUtc?.UtcDateTime ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task<DateTimeOffset?> GetLastAlertUtcAsync(string feedId, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"SELECT last_alert_utc FROM alert_state WHERE feed_id=@id;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);

        var result = await cmd.ExecuteScalarAsync(token);
        return result is null or DBNull ? null : new DateTimeOffset((DateTime)result, TimeSpan.Zero);
    }

    public async Task SetLastAlertUtcAsync(string feedId, DateTimeOffset utc, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        var sql = @"
INSERT INTO alert_state(feed_id, last_alert_utc) VALUES (@id, @ts)
ON CONFLICT(feed_id) DO UPDATE SET last_alert_utc=excluded.last_alert_utc;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", feedId);
        cmd.Parameters.AddWithValue("@ts", utc.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(token);
    }

    public async Task<long> DeleteSamplesOlderThanAsync(DateTimeOffset cutoffUtc, CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        const string sql = @"DELETE FROM samples WHERE ts_utc < @cutoff;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@cutoff", cutoffUtc.UtcDateTime);

        var rows = await cmd.ExecuteNonQueryAsync(token);
        return rows;
    }

    public async Task<List<GlobalRankSample>> GetGlobalRankSamplesAsync(
    DateTimeOffset fromUtc,
    CancellationToken token)
    {
        await using var conn = new NpgsqlConnection(this._cs);
        await conn.OpenAsync(token);

        // Minimal DB logic: just return the raw fields needed.
        // NOTE: "rank" is best quoted because it's a keyword-ish identifier in SQL contexts.
        const string sql = @"
SELECT
  ""rank"",
  listeners::double precision AS listeners
FROM samples
WHERE ts_utc >= @from
  AND ""rank"" IS NOT NULL;
";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@from", fromUtc.UtcDateTime);

        var rows = new List<GlobalRankSample>();

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            rows.Add(new GlobalRankSample(
                reader.GetInt32(0),
                reader.GetDouble(1)));
        }

        return rows;
    }

}
