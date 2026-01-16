package tech.flowcatalyst.platform.cors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Service for CORS origin lookups with non-blocking in-memory caching.
 *
 * Origins are cached for 5 minutes to avoid database hits on every request.
 * Uses a simple atomic reference cache that works on Vert.x event loop threads.
 */
@ApplicationScoped
public class CorsService {

    private static final Logger LOG = Logger.getLogger(CorsService.class);
    private static final long CACHE_TTL_SECONDS = 300; // 5 minutes

    @Inject
    CorsAllowedOriginRepository repository;

    // Simple non-blocking cache using AtomicReference
    private final AtomicReference<CacheEntry> cache = new AtomicReference<>();
    // Use ReentrantLock instead of synchronized to avoid pinning virtual threads
    private final ReentrantLock refreshLock = new ReentrantLock();

    /**
     * Check if an origin is allowed.
     * Uses cached origins for performance.
     */
    public boolean isOriginAllowed(String origin) {
        if (origin == null || origin.isBlank()) {
            return false;
        }
        return getAllowedOrigins().contains(origin);
    }

    /**
     * Get all allowed origins.
     * Cached for 5 minutes with automatic refresh.
     */
    public Set<String> getAllowedOrigins() {
        CacheEntry entry = cache.get();

        // Check if cache is valid
        if (entry != null && !entry.isExpired()) {
            return entry.origins;
        }

        // Reload from database
        return refreshCache();
    }

    /**
     * Invalidate the cache.
     * Called after add/delete operations.
     */
    public void invalidateCache() {
        cache.set(null);
        LOG.debug("CORS origins cache invalidated");
    }

    private Set<String> refreshCache() {
        refreshLock.lock();
        try {
            // Double-check after acquiring lock
            CacheEntry entry = cache.get();
            if (entry != null && !entry.isExpired()) {
                return entry.origins;
            }

            LOG.debug("Loading allowed origins from database");
            Set<String> origins = repository.listAll().stream()
                .map(o -> o.origin)
                .collect(Collectors.toSet());

            cache.set(new CacheEntry(origins));
            return origins;
        } finally {
            refreshLock.unlock();
        }
    }

    private static class CacheEntry {
        final Set<String> origins;
        final Instant expiresAt;

        CacheEntry(Set<String> origins) {
            this.origins = Set.copyOf(origins); // Immutable copy
            this.expiresAt = Instant.now().plusSeconds(CACHE_TTL_SECONDS);
        }

        boolean isExpired() {
            return Instant.now().isAfter(expiresAt);
        }
    }
}
