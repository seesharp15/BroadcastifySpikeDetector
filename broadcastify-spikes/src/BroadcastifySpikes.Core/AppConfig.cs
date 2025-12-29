namespace BroadcastifySpikes.Core;

public sealed record DbConfig(string Host, int Port, string Database, string User, string Password)
{
    public string ConnectionString =>
        $"Host={this.Host};Port={this.Port};Database={this.Database};Username={this.User};Password={this.Password};Pooling=true;Maximum Pool Size=50;";
}

public sealed record RedisConfig(string Host, int Port)
{
    public string ConnectionString => $"{this.Host}:{this.Port}";
}

public sealed record IngestConfig(Uri TopUrl, TimeSpan PollInterval);

public sealed record DetectConfig(
    TimeSpan PollInterval,
    TimeSpan LookbackWindow,
    double RobustZThreshold,
    double RecoveryZThreshold,
    int MinSamples,
    int PersistSamples,
    int NewFeedMinListeners,
    double GlobalRobustZThreshold,
    int GlobalMinSamples,
    TimeSpan GlobalLookbackWindow,
    TimeSpan MaxSampleAge);

public sealed record AlertConfig(TimeSpan SuppressWindow);

public sealed record AppConfig(
    string ServiceName,
    DbConfig Db,
    RedisConfig Redis,
    IngestConfig Ingest,
    DetectConfig Detect,
    AlertConfig Alert)
{
    public static AppConfig FromEnvironment()
    {
        string Get(string key, string fallback)
        {
            return Environment.GetEnvironmentVariable(key) ?? fallback;
        }

        int GetInt(string key, int fallback)
        {
            return int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;
        }

        double GetDouble(string key, double fallback)
        {
            return double.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;
        }

        var service = Get("SERVICE_NAME", "unknown");

        var pgHost = Get("POSTGRES_HOST", "localhost");
        var pgPort = GetInt("POSTGRES_PORT", 5432);
        var pgDb = Get("POSTGRES_DB", "broadcastify");
        var pgUser = Get("POSTGRES_USER", "broadcastify");
        var pgPw = Get("POSTGRES_PASSWORD", "broadcastify_local_pw");

        var redisHost = Get("REDIS_HOST", "localhost");
        var redisPort = GetInt("REDIS_PORT", 6379);

        var ingestPoll = TimeSpan.FromSeconds(GetInt("INGEST_POLL_SECONDS", 300));
        var detectPoll = TimeSpan.FromSeconds(GetInt("DETECT_POLL_SECONDS", 120));
        var lookbackDays = GetInt("LOOKBACK_DAYS", 3);

        var robustZ = GetDouble("ROBUST_Z", 3.5);
        var recoveryZ = GetDouble("RECOVERY_Z", 1.0);
        var minSamples = GetInt("MIN_SAMPLES", 40);
        var persist = GetInt("PERSIST_SAMPLES", 3);

        var globalRobustZThreshold = GetDouble("GLOBAL_ROBUST_Z", 3.0);
        var newFeedMinListeners = GetInt("GLOBAL_NEW_FEED_MIN_LISTENERS", 80);
        var globalMinSamples = GetInt("GLOBAL_MIN_SAMPLES", 300);
        var globalLookbackWindowDays = GetInt("GLOBAL_LOOKBACK_WINDOW_DAYS", 180);

        var maxSampleAgeDays = GetInt("MAX_SAMPLE_AGE_DAYS", 3);

        var suppressHrs = GetInt("ALERT_SUPPRESS_HOURS", 8);

        return new AppConfig(
            ServiceName: service,
            Db: new DbConfig(pgHost, pgPort, pgDb, pgUser, pgPw),
            Redis: new RedisConfig(redisHost, redisPort),
            Ingest: new IngestConfig(new Uri("https://m.broadcastify.com/listen/top"), ingestPoll),
            Detect: new DetectConfig(detectPoll, TimeSpan.FromDays(lookbackDays), robustZ, recoveryZ, minSamples, persist, newFeedMinListeners, globalRobustZThreshold, globalMinSamples, TimeSpan.FromDays(globalLookbackWindowDays), TimeSpan.FromDays(maxSampleAgeDays)),
            Alert: new AlertConfig(TimeSpan.FromHours(suppressHrs))
        );
    }
}
