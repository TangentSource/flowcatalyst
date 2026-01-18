package tech.flowcatalyst.platform.shared;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines all entity types in the system with their 3-character ID prefixes.
 *
 * Following the Stripe pattern, IDs are stored WITH the prefix in the database:
 * - Format: "{prefix}_{tsid}" (e.g., "clt_0HZXEQ5Y8JY5Z")
 * - Total length: 17 characters (3-char prefix + underscore + 13-char TSID)
 *
 * This provides:
 * - Self-documenting IDs (immediately know the entity type)
 * - Type safety (can't accidentally pass wrong ID type)
 * - Easier debugging and support
 * - No serialization/deserialization overhead
 * - Consistent format across API, database, and logs
 *
 * Usage:
 * <pre>
 * String id = TsidGenerator.generate(EntityType.CLIENT);  // "clt_0HZXEQ5Y8JY5Z"
 * </pre>
 */
public enum EntityType {

    // Core entities
    CLIENT("clt"),
    PRINCIPAL("prn"),
    APPLICATION("app"),
    SERVICE_ACCOUNT("sac"),

    // Authorization
    ROLE("rol"),
    PERMISSION("prm"),

    // Authentication
    OAUTH_CLIENT("oac"),
    AUTH_CODE("acd"),

    // Configuration
    CLIENT_AUTH_CONFIG("cac"),
    APP_CLIENT_CONFIG("apc"),
    IDP_ROLE_MAPPING("irm"),
    CORS_ORIGIN("cor"),
    ANCHOR_DOMAIN("anc"),

    // Access management
    CLIENT_ACCESS_GRANT("gnt"),

    // Events & Messaging
    EVENT_TYPE("evt"),
    EVENT("evn"),
    EVENT_READ("evr"),
    SUBSCRIPTION("sub"),
    DISPATCH_POOL("dpl"),
    DISPATCH_JOB("djb"),
    DISPATCH_JOB_READ("djr"),
    SCHEMA("sch"),

    // Audit
    AUDIT_LOG("aud");

    private final String prefix;

    private static final Map<String, EntityType> BY_PREFIX = new HashMap<>();

    static {
        for (EntityType type : values()) {
            BY_PREFIX.put(type.prefix, type);
        }
    }

    EntityType(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Returns the prefix used in serialized IDs (e.g., "client" for CLIENT).
     */
    public String prefix() {
        return prefix;
    }

    /**
     * Looks up an EntityType by its prefix.
     *
     * @param prefix the prefix string (e.g., "client")
     * @return the EntityType, or null if not found
     */
    public static EntityType fromPrefix(String prefix) {
        return BY_PREFIX.get(prefix);
    }
}
