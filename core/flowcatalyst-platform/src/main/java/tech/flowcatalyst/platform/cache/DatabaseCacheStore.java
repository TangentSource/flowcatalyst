package tech.flowcatalyst.platform.cache;

import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jooq.DSLContext;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.CacheEntries.CACHE_ENTRIES;

/**
 * Database-backed cache implementation using PostgreSQL and JOOQ.
 *
 * <p>Simple distributed caching without requiring Redis.
 * Good for multi-instance deployments where Redis isn't available.
 * Automatic cleanup of expired entries on read.
 *
 * <p>Note: @Typed excludes CacheStore from bean types so only the
 * CacheStoreProducer can provide the CacheStore interface.
 */
@Singleton
@Typed(DatabaseCacheStore.class)
public class DatabaseCacheStore implements CacheStore {

    @Inject
    DSLContext dsl;

    @Inject
    CacheConfig config;

    @Override
    public Optional<String> get(String cacheName, String key) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        return Optional.ofNullable(
            dsl.select(CACHE_ENTRIES.CACHE_VALUE)
                .from(CACHE_ENTRIES)
                .where(CACHE_ENTRIES.CACHE_NAME.eq(cacheName))
                .and(CACHE_ENTRIES.CACHE_KEY.eq(key))
                .and(CACHE_ENTRIES.EXPIRES_AT.gt(now))
                .fetchOne(CACHE_ENTRIES.CACHE_VALUE)
        );
    }

    @Override
    public void put(String cacheName, String key, String value) {
        put(cacheName, key, value, config.ttl());
    }

    @Override
    public void put(String cacheName, String key, String value, Duration ttl) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime expiresAt = now.plus(ttl);

        dsl.insertInto(CACHE_ENTRIES)
            .set(CACHE_ENTRIES.CACHE_NAME, cacheName)
            .set(CACHE_ENTRIES.CACHE_KEY, key)
            .set(CACHE_ENTRIES.CACHE_VALUE, value)
            .set(CACHE_ENTRIES.EXPIRES_AT, expiresAt)
            .set(CACHE_ENTRIES.CREATED_AT, now)
            .onConflict(CACHE_ENTRIES.CACHE_NAME, CACHE_ENTRIES.CACHE_KEY)
            .doUpdate()
            .set(CACHE_ENTRIES.CACHE_VALUE, value)
            .set(CACHE_ENTRIES.EXPIRES_AT, expiresAt)
            .execute();
    }

    @Override
    public void invalidate(String cacheName, String key) {
        dsl.deleteFrom(CACHE_ENTRIES)
            .where(CACHE_ENTRIES.CACHE_NAME.eq(cacheName))
            .and(CACHE_ENTRIES.CACHE_KEY.eq(key))
            .execute();
    }

    @Override
    public void invalidateAll(String cacheName) {
        dsl.deleteFrom(CACHE_ENTRIES)
            .where(CACHE_ENTRIES.CACHE_NAME.eq(cacheName))
            .execute();
    }

    /**
     * Cleanup expired entries. Can be called periodically via scheduler.
     *
     * @return Number of entries removed
     */
    public int cleanupExpired() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        return dsl.deleteFrom(CACHE_ENTRIES)
            .where(CACHE_ENTRIES.EXPIRES_AT.lt(now))
            .execute();
    }
}
