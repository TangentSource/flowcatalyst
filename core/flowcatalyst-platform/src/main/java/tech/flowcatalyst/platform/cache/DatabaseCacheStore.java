package tech.flowcatalyst.platform.cache;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jdbi.v3.core.Jdbi;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Database-backed cache implementation using PostgreSQL.
 *
 * <p>Simple distributed caching without requiring Redis.
 * Good for multi-instance deployments where Redis isn't available.
 * Automatic cleanup of expired entries on read.
 */
@Singleton
public class DatabaseCacheStore implements CacheStore {

    @Inject
    Jdbi jdbi;

    @Inject
    CacheConfig config;

    @Override
    public Optional<String> get(String cacheName, String key) {
        return jdbi.withHandle(handle -> {
            // Get value if not expired
            return handle.createQuery("""
                    SELECT cache_value FROM cache_entries
                    WHERE cache_name = :cacheName AND cache_key = :key AND expires_at > NOW()
                    """)
                .bind("cacheName", cacheName)
                .bind("key", key)
                .mapTo(String.class)
                .findFirst();
        });
    }

    @Override
    public void put(String cacheName, String key, String value) {
        put(cacheName, key, value, config.ttl());
    }

    @Override
    public void put(String cacheName, String key, String value, Duration ttl) {
        Instant expiresAt = Instant.now().plus(ttl);

        jdbi.useHandle(handle -> {
            handle.createUpdate("""
                    INSERT INTO cache_entries (cache_name, cache_key, cache_value, expires_at, created_at)
                    VALUES (:cacheName, :key, :value, :expiresAt, NOW())
                    ON CONFLICT (cache_name, cache_key)
                    DO UPDATE SET cache_value = :value, expires_at = :expiresAt
                    """)
                .bind("cacheName", cacheName)
                .bind("key", key)
                .bind("value", value)
                .bind("expiresAt", expiresAt)
                .execute();
        });
    }

    @Override
    public void invalidate(String cacheName, String key) {
        jdbi.useHandle(handle -> {
            handle.createUpdate("DELETE FROM cache_entries WHERE cache_name = :cacheName AND cache_key = :key")
                .bind("cacheName", cacheName)
                .bind("key", key)
                .execute();
        });
    }

    @Override
    public void invalidateAll(String cacheName) {
        jdbi.useHandle(handle -> {
            handle.createUpdate("DELETE FROM cache_entries WHERE cache_name = :cacheName")
                .bind("cacheName", cacheName)
                .execute();
        });
    }

    /**
     * Cleanup expired entries. Can be called periodically via scheduler.
     *
     * @return Number of entries removed
     */
    public int cleanupExpired() {
        return jdbi.withHandle(handle ->
            handle.createUpdate("DELETE FROM cache_entries WHERE expires_at < NOW()")
                .execute()
        );
    }
}
