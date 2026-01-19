package tech.flowcatalyst.serviceaccount.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.dispatchjob.model.SignatureAlgorithm;
import tech.flowcatalyst.platform.principal.Principal;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;
import tech.flowcatalyst.serviceaccount.entity.WebhookAuthType;
import tech.flowcatalyst.serviceaccount.entity.WebhookCredentials;
import tech.flowcatalyst.serviceaccount.repository.ServiceAccountFilter;
import tech.flowcatalyst.serviceaccount.repository.ServiceAccountRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ServiceAccountsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.ServiceAccounts.SERVICE_ACCOUNTS;

/**
 * JOOQ-based implementation of ServiceAccountRepository.
 */
@ApplicationScoped
public class JooqServiceAccountRepository implements ServiceAccountRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public ServiceAccount findById(String id) {
        return dsl.selectFrom(SERVICE_ACCOUNTS)
            .where(SERVICE_ACCOUNTS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<ServiceAccount> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<ServiceAccount> findByCode(String code) {
        return Optional.ofNullable(
            dsl.selectFrom(SERVICE_ACCOUNTS)
                .where(SERVICE_ACCOUNTS.CODE.eq(code))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<ServiceAccount> findByApplicationId(String applicationId) {
        return Optional.ofNullable(
            dsl.selectFrom(SERVICE_ACCOUNTS)
                .where(SERVICE_ACCOUNTS.APPLICATION_ID.eq(applicationId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<ServiceAccount> findByClientId(String clientId) {
        // Query array column for containing clientId
        return dsl.selectFrom(SERVICE_ACCOUNTS)
            .where(DSL.condition("? = ANY(client_ids)", clientId))
            .orderBy(SERVICE_ACCOUNTS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<ServiceAccount> findActive() {
        return dsl.selectFrom(SERVICE_ACCOUNTS)
            .where(SERVICE_ACCOUNTS.ACTIVE.eq(true))
            .orderBy(SERVICE_ACCOUNTS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<ServiceAccount> findWithFilter(ServiceAccountFilter filter) {
        Condition condition = DSL.noCondition();

        if (filter.clientId() != null) {
            condition = condition.and(DSL.condition("? = ANY(client_ids)", filter.clientId()));
        }
        if (filter.active() != null) {
            condition = condition.and(SERVICE_ACCOUNTS.ACTIVE.eq(filter.active()));
        }
        if (filter.applicationId() != null) {
            condition = condition.and(SERVICE_ACCOUNTS.APPLICATION_ID.eq(filter.applicationId()));
        }

        return dsl.selectFrom(SERVICE_ACCOUNTS)
            .where(condition)
            .orderBy(SERVICE_ACCOUNTS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<ServiceAccount> listAll() {
        return dsl.selectFrom(SERVICE_ACCOUNTS)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(SERVICE_ACCOUNTS)
            .fetchOne(0, Long.class);
    }

    @Override
    public long countWithFilter(ServiceAccountFilter filter) {
        Condition condition = DSL.noCondition();

        if (filter.clientId() != null) {
            condition = condition.and(DSL.condition("? = ANY(client_ids)", filter.clientId()));
        }
        if (filter.active() != null) {
            condition = condition.and(SERVICE_ACCOUNTS.ACTIVE.eq(filter.active()));
        }
        if (filter.applicationId() != null) {
            condition = condition.and(SERVICE_ACCOUNTS.APPLICATION_ID.eq(filter.applicationId()));
        }

        return dsl.selectCount()
            .from(SERVICE_ACCOUNTS)
            .where(condition)
            .fetchOne(0, Long.class);
    }

    @Override
    public void persist(ServiceAccount serviceAccount) {
        ServiceAccountsRecord record = toRecord(serviceAccount);
        record.setCreatedAt(toOffsetDateTime(serviceAccount.createdAt));
        record.setUpdatedAt(toOffsetDateTime(serviceAccount.updatedAt));
        dsl.insertInto(SERVICE_ACCOUNTS).set(record).execute();
    }

    @Override
    public void update(ServiceAccount serviceAccount) {
        serviceAccount.updatedAt = Instant.now();
        ServiceAccountsRecord record = toRecord(serviceAccount);
        record.setUpdatedAt(toOffsetDateTime(serviceAccount.updatedAt));
        dsl.update(SERVICE_ACCOUNTS)
            .set(record)
            .where(SERVICE_ACCOUNTS.ID.eq(serviceAccount.id))
            .execute();
    }

    @Override
    public void delete(ServiceAccount serviceAccount) {
        deleteById(serviceAccount.id);
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(SERVICE_ACCOUNTS)
            .where(SERVICE_ACCOUNTS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private ServiceAccount toDomain(Record record) {
        if (record == null) return null;

        ServiceAccount sa = new ServiceAccount();
        sa.id = record.get(SERVICE_ACCOUNTS.ID);
        sa.code = record.get(SERVICE_ACCOUNTS.CODE);
        sa.name = record.get(SERVICE_ACCOUNTS.NAME);
        sa.description = record.get(SERVICE_ACCOUNTS.DESCRIPTION);
        sa.applicationId = record.get(SERVICE_ACCOUNTS.APPLICATION_ID);
        sa.active = record.get(SERVICE_ACCOUNTS.ACTIVE);
        sa.lastUsedAt = toInstant(record.get(SERVICE_ACCOUNTS.LAST_USED_AT));
        sa.createdAt = toInstant(record.get(SERVICE_ACCOUNTS.CREATED_AT));
        sa.updatedAt = toInstant(record.get(SERVICE_ACCOUNTS.UPDATED_AT));

        // Client IDs array
        Object clientIdsObj = record.get(SERVICE_ACCOUNTS.CLIENT_IDS);
        if (clientIdsObj != null) {
            if (clientIdsObj instanceof String[]) {
                sa.clientIds = new ArrayList<>(Arrays.asList((String[]) clientIdsObj));
            } else if (clientIdsObj instanceof Object[]) {
                sa.clientIds = new ArrayList<>();
                for (Object o : (Object[]) clientIdsObj) {
                    if (o != null) sa.clientIds.add(o.toString());
                }
            }
        }

        // Webhook credentials (flat columns -> embedded object)
        WebhookCredentials wc = new WebhookCredentials();
        wc.authType = parseEnum(record.get(SERVICE_ACCOUNTS.WH_AUTH_TYPE), WebhookAuthType.class);
        wc.authTokenRef = record.get(SERVICE_ACCOUNTS.WH_AUTH_TOKEN_REF);
        wc.signingSecretRef = record.get(SERVICE_ACCOUNTS.WH_SIGNING_SECRET_REF);
        wc.signingAlgorithm = parseEnum(record.get(SERVICE_ACCOUNTS.WH_SIGNING_ALGORITHM), SignatureAlgorithm.class);
        wc.createdAt = toInstant(record.get(SERVICE_ACCOUNTS.WH_CREDENTIALS_CREATED_AT));
        wc.regeneratedAt = toInstant(record.get(SERVICE_ACCOUNTS.WH_CREDENTIALS_REGENERATED_AT));
        sa.webhookCredentials = wc;

        // Roles (JSONB array)
        String rolesJson = record.get(SERVICE_ACCOUNTS.ROLES);
        if (rolesJson != null && !rolesJson.isBlank()) {
            sa.roles = parseJson(rolesJson, new TypeReference<List<Principal.RoleAssignment>>() {});
            if (sa.roles == null) {
                sa.roles = new ArrayList<>();
            }
        }

        return sa;
    }

    private ServiceAccountsRecord toRecord(ServiceAccount sa) {
        ServiceAccountsRecord rec = new ServiceAccountsRecord();
        rec.setId(sa.id);
        rec.setCode(sa.code);
        rec.setName(sa.name);
        rec.setDescription(sa.description);
        rec.setApplicationId(sa.applicationId);
        rec.setActive(sa.active);
        rec.setLastUsedAt(toOffsetDateTime(sa.lastUsedAt));

        // Client IDs array
        if (sa.clientIds != null && !sa.clientIds.isEmpty()) {
            rec.setClientIds(sa.clientIds.toArray(new String[0]));
        }

        // Webhook credentials (embedded object -> flat columns)
        if (sa.webhookCredentials != null) {
            WebhookCredentials wc = sa.webhookCredentials;
            rec.setWhAuthType(wc.authType != null ? wc.authType.name() : WebhookAuthType.BEARER_TOKEN.name());
            rec.setWhAuthTokenRef(wc.authTokenRef);
            rec.setWhSigningSecretRef(wc.signingSecretRef);
            rec.setWhSigningAlgorithm(wc.signingAlgorithm != null ? wc.signingAlgorithm.name() : SignatureAlgorithm.HMAC_SHA256.name());
            rec.setWhCredentialsCreatedAt(toOffsetDateTime(wc.createdAt));
            rec.setWhCredentialsRegeneratedAt(toOffsetDateTime(wc.regeneratedAt));
        }

        // Roles -> JSONB array
        rec.setRoles(toJson(sa.roles != null ? sa.roles : new ArrayList<>()));

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
