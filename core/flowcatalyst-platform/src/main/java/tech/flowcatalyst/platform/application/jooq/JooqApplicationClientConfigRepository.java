package tech.flowcatalyst.platform.application.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.application.ApplicationClientConfig;
import tech.flowcatalyst.platform.application.ApplicationClientConfigRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ApplicationClientConfigsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.ApplicationClientConfigs.APPLICATION_CLIENT_CONFIGS;

/**
 * JOOQ-based implementation of ApplicationClientConfigRepository.
 */
@ApplicationScoped
public class JooqApplicationClientConfigRepository implements ApplicationClientConfigRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public Optional<ApplicationClientConfig> findByIdOptional(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
                .where(APPLICATION_CLIENT_CONFIGS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<ApplicationClientConfig> findByApplicationAndClient(String applicationId, String clientId) {
        return Optional.ofNullable(
            dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
                .where(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID.eq(applicationId))
                .and(APPLICATION_CLIENT_CONFIGS.CLIENT_ID.eq(clientId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<ApplicationClientConfig> findByApplication(String applicationId) {
        return dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID.eq(applicationId))
            .fetch(this::toDomain);
    }

    @Override
    public List<ApplicationClientConfig> findByClient(String clientId) {
        return dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.CLIENT_ID.eq(clientId))
            .fetch(this::toDomain);
    }

    @Override
    public List<ApplicationClientConfig> findEnabledByClient(String clientId) {
        return dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.CLIENT_ID.eq(clientId))
            .and(APPLICATION_CLIENT_CONFIGS.ENABLED.eq(true))
            .fetch(this::toDomain);
    }

    @Override
    public boolean isApplicationEnabledForClient(String applicationId, String clientId) {
        return dsl.fetchExists(
            dsl.selectFrom(APPLICATION_CLIENT_CONFIGS)
                .where(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID.eq(applicationId))
                .and(APPLICATION_CLIENT_CONFIGS.CLIENT_ID.eq(clientId))
                .and(APPLICATION_CLIENT_CONFIGS.ENABLED.eq(true))
        );
    }

    @Override
    public long countByApplication(String applicationId) {
        return dsl.selectCount()
            .from(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID.eq(applicationId))
            .fetchOne(0, Long.class);
    }

    @Override
    public void persist(ApplicationClientConfig config) {
        ApplicationClientConfigsRecord record = toRecord(config);
        record.setCreatedAt(toOffsetDateTime(config.createdAt));
        record.setUpdatedAt(toOffsetDateTime(config.updatedAt));
        dsl.insertInto(APPLICATION_CLIENT_CONFIGS).set(record).execute();
    }

    @Override
    public void update(ApplicationClientConfig config) {
        config.updatedAt = Instant.now();
        ApplicationClientConfigsRecord record = toRecord(config);
        record.setUpdatedAt(toOffsetDateTime(config.updatedAt));
        dsl.update(APPLICATION_CLIENT_CONFIGS)
            .set(record)
            .where(APPLICATION_CLIENT_CONFIGS.ID.eq(config.id))
            .execute();
    }

    @Override
    public void delete(ApplicationClientConfig config) {
        dsl.deleteFrom(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.ID.eq(config.id))
            .execute();
    }

    @Override
    public void deleteByApplicationAndClient(String applicationId, String clientId) {
        dsl.deleteFrom(APPLICATION_CLIENT_CONFIGS)
            .where(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID.eq(applicationId))
            .and(APPLICATION_CLIENT_CONFIGS.CLIENT_ID.eq(clientId))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private ApplicationClientConfig toDomain(Record record) {
        if (record == null) return null;

        ApplicationClientConfig c = new ApplicationClientConfig();
        c.id = record.get(APPLICATION_CLIENT_CONFIGS.ID);
        c.applicationId = record.get(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID);
        c.clientId = record.get(APPLICATION_CLIENT_CONFIGS.CLIENT_ID);
        c.enabled = record.get(APPLICATION_CLIENT_CONFIGS.ENABLED);
        c.baseUrlOverride = record.get(APPLICATION_CLIENT_CONFIGS.BASE_URL_OVERRIDE);
        c.websiteOverride = record.get(APPLICATION_CLIENT_CONFIGS.WEBSITE_OVERRIDE);
        c.createdAt = toInstant(record.get(APPLICATION_CLIENT_CONFIGS.CREATED_AT));
        c.updatedAt = toInstant(record.get(APPLICATION_CLIENT_CONFIGS.UPDATED_AT));

        // Config JSON
        String configJson = record.get(APPLICATION_CLIENT_CONFIGS.CONFIG_JSON);
        if (configJson != null && !configJson.isBlank()) {
            c.configJson = parseJson(configJson, new TypeReference<Map<String, Object>>() {});
        }

        return c;
    }

    private ApplicationClientConfigsRecord toRecord(ApplicationClientConfig c) {
        ApplicationClientConfigsRecord rec = new ApplicationClientConfigsRecord();
        rec.setId(c.id);
        rec.setApplicationId(c.applicationId);
        rec.setClientId(c.clientId);
        rec.setEnabled(c.enabled);
        rec.setBaseUrlOverride(c.baseUrlOverride);
        rec.setWebsiteOverride(c.websiteOverride);
        rec.setConfigJson(toJson(c.configJson));
        return rec;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }

    private <T> T parseJson(String json, TypeReference<T> typeRef) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, typeRef);
        } catch (Exception e) {
            return null;
        }
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return null;
        }
    }
}
