package tech.flowcatalyst.platform.shared;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Centralized utility for typed ID serialization/deserialization.
 *
 * Provides consistent ID handling across all APIs following the Stripe pattern:
 * - External format: "{prefix}_{tsid}" (e.g., "client_0HZXEQ5Y8JY5Z")
 * - Internal format: raw TSID string (e.g., "0HZXEQ5Y8JY5Z")
 *
 * Features:
 * - Type-safe serialization and deserialization
 * - Validation with detailed error messages
 * - Metrics for monitoring invalid ID attempts
 * - Batch operations for collections
 *
 * Usage in Resources:
 * <pre>
 * {@code
 * @Inject TypedId typedId;
 *
 * // In DTO mapping
 * String externalId = typedId.serialize(EntityType.CLIENT, client.id);
 *
 * // In path param handling
 * String internalId = typedId.deserialize(EntityType.CLIENT, pathId);
 * }
 * </pre>
 *
 * For static contexts (e.g., records), use the Ops inner class:
 * <pre>
 * {@code
 * String externalId = TypedId.Ops.serialize(EntityType.CLIENT, client.id);
 * }
 * </pre>
 */
@ApplicationScoped
public class TypedId {

    private static final Logger LOG = Logger.getLogger(TypedId.class);

    /**
     * Separator between prefix and ID.
     */
    public static final String SEPARATOR = "_";

    /**
     * Pattern for valid TSID strings (Crockford Base32, 13 chars).
     * Crockford Base32 uses: 0-9, A-Z excluding I, L, O, U
     */
    private static final Pattern TSID_PATTERN = Pattern.compile("^[0-9A-HJKMNP-TV-Z]{13}$", Pattern.CASE_INSENSITIVE);

    @Inject
    MeterRegistry registry;

    private Counter parseSuccessCounter;
    private Counter parseFailureCounter;

    @PostConstruct
    void init() {
        parseSuccessCounter = Counter.builder("flowcatalyst.typed_id.parse")
            .tag("result", "success")
            .description("Count of successful typed ID parse operations")
            .register(registry);

        parseFailureCounter = Counter.builder("flowcatalyst.typed_id.parse")
            .tag("result", "failure")
            .description("Count of failed typed ID parse operations")
            .register(registry);
    }

    // ========================================================================
    // Instance methods (with metrics)
    // ========================================================================

    /**
     * Serializes an internal ID to external typed format.
     *
     * @param type the entity type
     * @param internalId the raw TSID string
     * @return the typed ID (e.g., "client_0HZXEQ5Y8JY5Z")
     * @throws IllegalArgumentException if internalId is null or empty
     */
    public String serialize(EntityType type, String internalId) {
        return Ops.serialize(type, internalId);
    }

    /**
     * Deserializes an external typed ID to internal format.
     * Records metrics for success/failure.
     *
     * @param expectedType the expected entity type
     * @param externalId the typed ID (e.g., "client_0HZXEQ5Y8JY5Z")
     * @return the raw TSID string
     * @throws InvalidTypedIdException if the ID is malformed or wrong type
     */
    public String deserialize(EntityType expectedType, String externalId) {
        try {
            String result = Ops.deserialize(expectedType, externalId);
            if (parseSuccessCounter != null) {
                parseSuccessCounter.increment();
            }
            return result;
        } catch (InvalidTypedIdException e) {
            if (parseFailureCounter != null) {
                parseFailureCounter.increment();
            }
            recordParseFailure(expectedType, externalId, e.getReason());
            throw e;
        }
    }

    /**
     * Attempts to deserialize, returning null on failure instead of throwing.
     * Useful for optional ID fields.
     *
     * @param expectedType the expected entity type
     * @param externalId the typed ID, may be null
     * @return the raw TSID string, or null if input is null or invalid
     */
    public String deserializeOrNull(EntityType expectedType, String externalId) {
        if (externalId == null || externalId.isBlank()) {
            return null;
        }
        try {
            return deserialize(expectedType, externalId);
        } catch (InvalidTypedIdException e) {
            LOG.debugf("Optional ID parse failed: %s", e.getMessage());
            return null;
        }
    }

    /**
     * Validates a typed ID without deserializing.
     *
     * @param expectedType the expected entity type
     * @param externalId the typed ID to validate
     * @return true if valid, false otherwise
     */
    public boolean isValid(EntityType expectedType, String externalId) {
        return Ops.isValid(expectedType, externalId);
    }

    /**
     * Serializes a list of internal IDs.
     */
    public List<String> serializeAll(EntityType type, List<String> internalIds) {
        return Ops.serializeAll(type, internalIds);
    }

    /**
     * Deserializes a list of typed IDs.
     *
     * @throws InvalidTypedIdException if any ID is invalid
     */
    public List<String> deserializeAll(EntityType expectedType, List<String> externalIds) {
        return externalIds.stream()
            .map(id -> deserialize(expectedType, id))
            .toList();
    }

    private void recordParseFailure(EntityType expectedType, String externalId, String reason) {
        // Log for debugging/alerting
        LOG.warnf("Invalid typed ID: expected=%s, value='%s', reason=%s",
            expectedType, maskId(externalId), reason);

        // Record detailed metric
        if (registry != null) {
            registry.counter("flowcatalyst.typed_id.parse.errors",
                "expected_type", expectedType.name(),
                "reason", reason
            ).increment();
        }
    }

    private String maskId(String id) {
        if (id == null) return "null";
        if (id.length() <= 8) return id;
        return id.substring(0, 8) + "...";
    }

    // ========================================================================
    // Static operations (for use in records/DTOs without injection)
    // ========================================================================

    /**
     * Static operations for typed IDs.
     * Use these in contexts where dependency injection is not available
     * (e.g., record constructors, static factory methods).
     *
     * Note: These methods do not record metrics. For monitored operations,
     * inject TypedId and use instance methods.
     */
    public static final class Ops {

        private Ops() {
            // Static utility class
        }

        /**
         * Serializes an internal ID to external typed format.
         *
         * @param type the entity type
         * @param internalId the raw TSID string
         * @return the typed ID (e.g., "client_0HZXEQ5Y8JY5Z"), or null if internalId is null
         */
        public static String serialize(EntityType type, String internalId) {
            Objects.requireNonNull(type, "EntityType must not be null");
            if (internalId == null) {
                return null;
            }
            return type.prefix() + SEPARATOR + internalId;
        }

        /**
         * Deserializes an external typed ID to internal format.
         *
         * @param expectedType the expected entity type
         * @param externalId the typed ID (e.g., "client_0HZXEQ5Y8JY5Z")
         * @return the raw TSID string
         * @throws InvalidTypedIdException if the ID is malformed or wrong type
         */
        public static String deserialize(EntityType expectedType, String externalId) {
            Objects.requireNonNull(expectedType, "EntityType must not be null");

            if (externalId == null || externalId.isBlank()) {
                throw new InvalidTypedIdException(
                    "ID is required",
                    expectedType,
                    externalId,
                    "empty"
                );
            }

            int separatorIndex = externalId.indexOf(SEPARATOR);
            if (separatorIndex == -1) {
                throw new InvalidTypedIdException(
                    String.format("Invalid ID format. Expected '%s_<id>' but got '%s'",
                        expectedType.prefix(), externalId),
                    expectedType,
                    externalId,
                    "missing_separator"
                );
            }

            String prefix = externalId.substring(0, separatorIndex);
            String rawId = externalId.substring(separatorIndex + 1);

            // Check prefix matches expected type
            EntityType actualType = EntityType.fromPrefix(prefix);
            if (actualType == null) {
                throw new InvalidTypedIdException(
                    String.format("Unknown ID prefix '%s'. Valid prefixes: %s",
                        prefix, getValidPrefixesFor(expectedType)),
                    expectedType,
                    externalId,
                    "unknown_prefix"
                );
            }

            if (actualType != expectedType) {
                throw new InvalidTypedIdException(
                    String.format("ID type mismatch. Expected '%s' but got '%s'",
                        expectedType.prefix(), actualType.prefix()),
                    expectedType,
                    externalId,
                    "type_mismatch"
                );
            }

            // Validate TSID format
            if (!isValidTsid(rawId)) {
                throw new InvalidTypedIdException(
                    String.format("Invalid TSID format in ID '%s'", externalId),
                    expectedType,
                    externalId,
                    "invalid_tsid"
                );
            }

            return rawId;
        }

        /**
         * Validates a typed ID without throwing exceptions.
         *
         * @param expectedType the expected entity type
         * @param externalId the typed ID to validate
         * @return true if valid, false otherwise
         */
        public static boolean isValid(EntityType expectedType, String externalId) {
            if (externalId == null || externalId.isBlank()) {
                return false;
            }

            int separatorIndex = externalId.indexOf(SEPARATOR);
            if (separatorIndex == -1) {
                return false;
            }

            String prefix = externalId.substring(0, separatorIndex);
            String rawId = externalId.substring(separatorIndex + 1);

            EntityType actualType = EntityType.fromPrefix(prefix);
            if (actualType != expectedType) {
                return false;
            }

            return isValidTsid(rawId);
        }

        /**
         * Extracts the entity type from a typed ID.
         *
         * @param externalId the typed ID
         * @return the entity type, or null if not recognized
         */
        public static EntityType extractType(String externalId) {
            if (externalId == null || externalId.isBlank()) {
                return null;
            }

            int separatorIndex = externalId.indexOf(SEPARATOR);
            if (separatorIndex == -1) {
                return null;
            }

            String prefix = externalId.substring(0, separatorIndex);
            return EntityType.fromPrefix(prefix);
        }

        /**
         * Serializes a list of internal IDs.
         */
        public static List<String> serializeAll(EntityType type, List<String> internalIds) {
            if (internalIds == null) {
                return List.of();
            }
            return internalIds.stream()
                .map(id -> serialize(type, id))
                .toList();
        }

        /**
         * Deserializes a list of typed IDs.
         *
         * @throws InvalidTypedIdException if any ID is invalid
         */
        public static List<String> deserializeAll(EntityType expectedType, List<String> externalIds) {
            if (externalIds == null) {
                return List.of();
            }
            return externalIds.stream()
                .map(id -> deserialize(expectedType, id))
                .toList();
        }

        private static boolean isValidTsid(String id) {
            return id != null && TSID_PATTERN.matcher(id).matches();
        }

        private static String getValidPrefixesFor(EntityType type) {
            return type.prefix();
        }
    }

    // ========================================================================
    // Exception
    // ========================================================================

    /**
     * Exception thrown when a typed ID cannot be parsed.
     */
    public static class InvalidTypedIdException extends IllegalArgumentException {

        private final EntityType expectedType;
        private final String providedValue;
        private final String reason;

        public InvalidTypedIdException(String message, EntityType expectedType, String providedValue, String reason) {
            super(message);
            this.expectedType = expectedType;
            this.providedValue = providedValue;
            this.reason = reason;
        }

        public EntityType getExpectedType() {
            return expectedType;
        }

        public String getProvidedValue() {
            return providedValue;
        }

        public String getReason() {
            return reason;
        }
    }
}
