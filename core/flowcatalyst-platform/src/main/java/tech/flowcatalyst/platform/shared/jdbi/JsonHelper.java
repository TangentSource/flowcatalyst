package tech.flowcatalyst.platform.shared.jdbi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for JSON serialization/deserialization in JDBI repositories.
 * Used for JSONB column handling in PostgreSQL.
 */
public class JsonHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Convert an object to JSON string for storing in JSONB column.
     *
     * @param obj the object to serialize (can be null)
     * @return JSON string or null if input is null
     */
    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }

    /**
     * Parse a JSON string to an object.
     *
     * @param json  the JSON string (can be null)
     * @param clazz the target class
     * @param <T>   the target type
     * @return parsed object or null if input is null
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse JSON: " + json, e);
        }
    }

    /**
     * Parse a JSON array string to a list.
     *
     * @param json  the JSON array string (can be null)
     * @param clazz the element class
     * @param <T>   the element type
     * @return parsed list or empty list if input is null/empty
     */
    public static <T> List<T> fromJsonList(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return new ArrayList<>();
        }
        try {
            return MAPPER.readValue(json,
                    MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse JSON list: " + json, e);
        }
    }

    /**
     * Parse a JSON string using a TypeReference for complex generic types.
     *
     * @param json    the JSON string (can be null)
     * @param typeRef the type reference
     * @param <T>     the target type
     * @return parsed object or null if input is null
     */
    public static <T> T fromJson(String json, TypeReference<T> typeRef) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse JSON: " + json, e);
        }
    }

    /**
     * Convert a list to JSON string, returning "[]" for null/empty lists.
     *
     * @param list the list to serialize
     * @return JSON array string
     */
    public static String toJsonArray(List<?> list) {
        if (list == null || list.isEmpty()) {
            return "[]";
        }
        return toJson(list);
    }

    /**
     * Get the shared ObjectMapper instance.
     *
     * @return the ObjectMapper
     */
    public static ObjectMapper getMapper() {
        return MAPPER;
    }
}
