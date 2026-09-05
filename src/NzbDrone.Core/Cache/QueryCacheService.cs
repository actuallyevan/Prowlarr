using System;
using System.IO.Compression;
using System.Threading.Tasks;
using Dapper;
using NLog;

namespace NzbDrone.Core.Cache
{
    public class QueryCacheService(ISqliteCacheDatabase cacheDatabase, Logger logger)
    {
        private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(
            int.TryParse(Environment.GetEnvironmentVariable("CACHE_TTL_MINS"), out var mins) && mins > 0
                ? mins
                : 10);

        public async ValueTask<byte[]> GetAsync(string key)
        {
            var hash = CacheKeyHasher.Hash(key);
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            try
            {
                using var connection = cacheDatabase.OpenConnection();

                const string selectSql = "SELECT Payload, CreatedAt FROM QueryCache WHERE KeyHash = @hash;";
                var entry = await connection.QueryFirstOrDefaultAsync<QueryCacheRecord>(selectSql, new { hash });

                if (entry?.Payload == null || entry.Payload.Length == 0 || (entry.CreatedAt + (long)CacheTtl.TotalSeconds) <= now)
                {
                    return null;
                }

                return BrotliCompressionHelper.Decompress(entry.Payload);
            }
            catch (Exception ex)
            {
                logger.Warn(ex, "Failed to read QueryCache for key: {0}", key);
                return null;
            }
        }

        public async ValueTask SetAsync(string key, byte[] value)
        {
            if (value == null || value.Length == 0)
            {
                return;
            }

            var hash = CacheKeyHasher.Hash(key);

            try
            {
                var compressed = BrotliCompressionHelper.Compress(value, CompressionLevel.Fastest);
                var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                using var connection = cacheDatabase.OpenConnection();

                const string upsertSql = @"
                    INSERT INTO QueryCache (KeyHash, OriginalKey, Payload, CompressedSize, UncompressedSize, CreatedAt)
                    VALUES (@hash, @key, @compressed, @compressedSize, @uncompressedSize, @now)
                    ON CONFLICT(KeyHash) DO UPDATE SET
                        OriginalKey = @key,
                        Payload = @compressed,
                        CompressedSize = @compressedSize,
                        UncompressedSize = @uncompressedSize,
                        CreatedAt = @now;
                ";

                await connection.ExecuteAsync(upsertSql, new
                {
                    hash,
                    key,
                    compressed,
                    compressedSize = compressed.Length,
                    uncompressedSize = value.Length,
                    now
                });
            }
            catch (Exception ex)
            {
                logger.Warn(ex, "Failed to store into QueryCache for key: {0}", key);
            }
        }

        public void Cleanup()
        {
            try
            {
                var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var threshold = now - (long)CacheTtl.TotalSeconds;

                using var connection = cacheDatabase.OpenConnection();
                var expiredOutputCount =
                    connection.Execute("DELETE FROM QueryCache WHERE CreatedAt <= @threshold;", new { threshold });

                if (expiredOutputCount > 0)
                {
                    logger.Debug("Evicted {0} expired records from QueryCache", expiredOutputCount);
                }
            }
            catch (Exception ex)
            {
                logger.Warn(ex, "Failed to evict expired records from QueryCache");
            }
        }

        private class QueryCacheRecord
        {
            public byte[] Payload { get; set; }
            public long CreatedAt { get; set; }
        }
    }
}
