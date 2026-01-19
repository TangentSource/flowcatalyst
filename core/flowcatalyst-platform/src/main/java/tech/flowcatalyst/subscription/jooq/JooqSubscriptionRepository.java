package tech.flowcatalyst.subscription.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.subscription.*;
import tech.flowcatalyst.platform.jooq.generated.tables.records.SubscriptionsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.Subscriptions.SUBSCRIPTIONS;

/**
 * JOOQ-based implementation of SubscriptionRepository.
 */
@ApplicationScoped
public class JooqSubscriptionRepository implements SubscriptionRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public Subscription findById(String id) {
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(SUBSCRIPTIONS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<Subscription> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<Subscription> findByCodeAndClient(String code, String clientId) {
        Condition condition = SUBSCRIPTIONS.CODE.eq(code);
        if (clientId == null) {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.eq(clientId));
        }
        return Optional.ofNullable(
            dsl.selectFrom(SUBSCRIPTIONS)
                .where(condition)
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Subscription> findByClientId(String clientId) {
        Condition condition = clientId == null
            ? SUBSCRIPTIONS.CLIENT_ID.isNull()
            : SUBSCRIPTIONS.CLIENT_ID.eq(clientId);
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(condition)
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findAnchorLevel() {
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(SUBSCRIPTIONS.CLIENT_ID.isNull())
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findByDispatchPoolId(String dispatchPoolId) {
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(SUBSCRIPTIONS.DISPATCH_POOL_ID.eq(dispatchPoolId))
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findByEventTypeId(String eventTypeId) {
        // Query JSONB array for matching eventTypeId
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(DSL.condition(
                "event_types::jsonb @> ?::jsonb",
                "[{\"eventTypeId\":\"" + eventTypeId + "\"}]"
            ))
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findByStatus(SubscriptionStatus status) {
        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(SUBSCRIPTIONS.STATUS.eq(status.name()))
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findActive() {
        return findByStatus(SubscriptionStatus.ACTIVE);
    }

    @Override
    public List<Subscription> findWithFilters(String clientId, SubscriptionStatus status, SubscriptionSource source, String dispatchPoolId) {
        Condition condition = DSL.noCondition();

        if (clientId != null) {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.eq(clientId));
        }
        if (status != null) {
            condition = condition.and(SUBSCRIPTIONS.STATUS.eq(status.name()));
        }
        if (source != null) {
            condition = condition.and(SUBSCRIPTIONS.SOURCE.eq(source.name()));
        }
        if (dispatchPoolId != null) {
            condition = condition.and(SUBSCRIPTIONS.DISPATCH_POOL_ID.eq(dispatchPoolId));
        }

        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(condition)
            .orderBy(SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findActiveByEventTypeAndClient(String eventTypeId, String clientId) {
        Condition condition = SUBSCRIPTIONS.STATUS.eq(SubscriptionStatus.ACTIVE.name())
            .and(DSL.condition(
                "event_types::jsonb @> ?::jsonb",
                "[{\"eventTypeId\":\"" + eventTypeId + "\"}]"
            ));

        if (clientId == null) {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.eq(clientId));
        }

        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(condition)
            .orderBy(SUBSCRIPTIONS.SEQUENCE, SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> findActiveByEventTypeCodeAndClient(String eventTypeCode, String clientId) {
        Condition condition = SUBSCRIPTIONS.STATUS.eq(SubscriptionStatus.ACTIVE.name())
            .and(DSL.condition(
                "event_types::jsonb @> ?::jsonb",
                "[{\"eventTypeCode\":\"" + eventTypeCode + "\"}]"
            ));

        if (clientId == null) {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.eq(clientId));
        }

        return dsl.selectFrom(SUBSCRIPTIONS)
            .where(condition)
            .orderBy(SUBSCRIPTIONS.SEQUENCE, SUBSCRIPTIONS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<Subscription> listAll() {
        return dsl.selectFrom(SUBSCRIPTIONS)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(SUBSCRIPTIONS)
            .fetchOne(0, Long.class);
    }

    @Override
    public boolean existsByCodeAndClient(String code, String clientId) {
        Condition condition = SUBSCRIPTIONS.CODE.eq(code);
        if (clientId == null) {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(SUBSCRIPTIONS.CLIENT_ID.eq(clientId));
        }
        return dsl.fetchExists(
            dsl.selectFrom(SUBSCRIPTIONS).where(condition)
        );
    }

    @Override
    public boolean existsByDispatchPoolId(String dispatchPoolId) {
        return dsl.fetchExists(
            dsl.selectFrom(SUBSCRIPTIONS)
                .where(SUBSCRIPTIONS.DISPATCH_POOL_ID.eq(dispatchPoolId))
        );
    }

    @Override
    public void persist(Subscription subscription) {
        SubscriptionsRecord record = toRecord(subscription);
        record.setCreatedAt(toOffsetDateTime(subscription.createdAt()));
        record.setUpdatedAt(toOffsetDateTime(subscription.updatedAt()));
        dsl.insertInto(SUBSCRIPTIONS).set(record).execute();
    }

    @Override
    public void update(Subscription subscription) {
        SubscriptionsRecord record = toRecord(subscription);
        record.setUpdatedAt(toOffsetDateTime(Instant.now()));
        dsl.update(SUBSCRIPTIONS)
            .set(record)
            .where(SUBSCRIPTIONS.ID.eq(subscription.id()))
            .execute();
    }

    @Override
    public void delete(Subscription subscription) {
        deleteById(subscription.id());
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(SUBSCRIPTIONS)
            .where(SUBSCRIPTIONS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Subscription toDomain(Record record) {
        if (record == null) return null;

        String eventTypesJson = record.get(SUBSCRIPTIONS.EVENT_TYPES);
        List<EventTypeBinding> eventTypes = new ArrayList<>();
        if (eventTypesJson != null && !eventTypesJson.isBlank()) {
            eventTypes = parseJson(eventTypesJson, new TypeReference<List<EventTypeBinding>>() {});
            if (eventTypes == null) {
                eventTypes = new ArrayList<>();
            }
        }

        String customConfigJson = record.get(SUBSCRIPTIONS.CUSTOM_CONFIG);
        List<ConfigEntry> customConfig = new ArrayList<>();
        if (customConfigJson != null && !customConfigJson.isBlank()) {
            customConfig = parseJson(customConfigJson, new TypeReference<List<ConfigEntry>>() {});
            if (customConfig == null) {
                customConfig = new ArrayList<>();
            }
        }

        return new Subscription(
            record.get(SUBSCRIPTIONS.ID),
            record.get(SUBSCRIPTIONS.CODE),
            record.get(SUBSCRIPTIONS.NAME),
            record.get(SUBSCRIPTIONS.DESCRIPTION),
            record.get(SUBSCRIPTIONS.CLIENT_ID),
            record.get(SUBSCRIPTIONS.CLIENT_IDENTIFIER),
            eventTypes,
            record.get(SUBSCRIPTIONS.TARGET),
            record.get(SUBSCRIPTIONS.QUEUE),
            customConfig,
            parseEnum(record.get(SUBSCRIPTIONS.SOURCE), SubscriptionSource.class),
            parseEnum(record.get(SUBSCRIPTIONS.STATUS), SubscriptionStatus.class),
            record.get(SUBSCRIPTIONS.MAX_AGE_SECONDS),
            record.get(SUBSCRIPTIONS.DISPATCH_POOL_ID),
            record.get(SUBSCRIPTIONS.DISPATCH_POOL_CODE),
            record.get(SUBSCRIPTIONS.DELAY_SECONDS),
            record.get(SUBSCRIPTIONS.SEQUENCE),
            parseEnum(record.get(SUBSCRIPTIONS.MODE), DispatchMode.class),
            record.get(SUBSCRIPTIONS.TIMEOUT_SECONDS),
            record.get(SUBSCRIPTIONS.MAX_RETRIES),
            record.get(SUBSCRIPTIONS.SERVICE_ACCOUNT_ID),
            record.get(SUBSCRIPTIONS.DATA_ONLY),
            toInstant(record.get(SUBSCRIPTIONS.CREATED_AT)),
            toInstant(record.get(SUBSCRIPTIONS.UPDATED_AT))
        );
    }

    private SubscriptionsRecord toRecord(Subscription s) {
        SubscriptionsRecord rec = new SubscriptionsRecord();
        rec.setId(s.id());
        rec.setCode(s.code());
        rec.setName(s.name());
        rec.setDescription(s.description());
        rec.setClientId(s.clientId());
        rec.setClientIdentifier(s.clientIdentifier());
        rec.setTarget(s.target());
        rec.setQueue(s.queue());
        rec.setSource(s.source() != null ? s.source().name() : SubscriptionSource.API.name());
        rec.setStatus(s.status() != null ? s.status().name() : SubscriptionStatus.ACTIVE.name());
        rec.setMaxAgeSeconds(s.maxAgeSeconds());
        rec.setDispatchPoolId(s.dispatchPoolId());
        rec.setDispatchPoolCode(s.dispatchPoolCode());
        rec.setDelaySeconds(s.delaySeconds());
        rec.setSequence(s.sequence());
        rec.setMode(s.mode() != null ? s.mode().name() : DispatchMode.IMMEDIATE.name());
        rec.setTimeoutSeconds(s.timeoutSeconds());
        rec.setMaxRetries(s.maxRetries());
        rec.setServiceAccountId(s.serviceAccountId());
        rec.setDataOnly(s.dataOnly());

        // JSONB arrays
        rec.setEventTypes(toJson(s.eventTypes() != null ? s.eventTypes() : new ArrayList<>()));
        rec.setCustomConfig(toJson(s.customConfig() != null ? s.customConfig() : new ArrayList<>()));

        return rec;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private <E extends Enum<E>> E parseEnum(String value, Class<E> enumClass) {
        if (value == null) return null;
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

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
        } catch (Exception ex) {
            return null;
        }
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception ex) {
            return null;
        }
    }
}
