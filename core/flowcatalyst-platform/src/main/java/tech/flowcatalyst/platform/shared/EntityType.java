package tech.flowcatalyst.platform.shared;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines all entity types in the system with their ID prefixes.
 *
 * Following the Stripe/OpenAI pattern, IDs are serialized as "{prefix}_{tsid}"
 * for external APIs. This provides:
 *
 * - Self-documenting IDs (immediately know the entity type)
 * - Type safety (can't accidentally pass wrong ID type)
 * - Easier debugging and support
 * - Clear API contracts
 *
 * Example: "client_0HZXEQ5Y8JY5Z" instead of just "0HZXEQ5Y8JY5Z"
 *
 * Usage:
 * <pre>
 * String externalId = TypedId.serialize(EntityType.CLIENT, internalId);
 * String internalId = TypedId.deserialize(EntityType.CLIENT, externalId);
 * </pre>
 */
public enum EntityType {

    // Core entities
    CLIENT("client"),
    PRINCIPAL("principal"),
    APPLICATION("app"),

    // Authorization
    ROLE("role"),
    PERMISSION("perm"),

    // Authentication
    OAUTH_CLIENT("oauth"),
    AUTH_CODE("authcode"),

    // Configuration
    CLIENT_AUTH_CONFIG("authcfg"),
    APP_CLIENT_CONFIG("appcfg"),
    IDP_ROLE_MAPPING("idpmap"),
    CORS_ORIGIN("cors"),
    ANCHOR_DOMAIN("anchor"),

    // Access management
    CLIENT_ACCESS_GRANT("grant"),

    // Audit
    AUDIT_LOG("audit");

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
