package tech.flowcatalyst.platform.client.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.AuthProvider;
import tech.flowcatalyst.platform.client.AuthConfigType;
import tech.flowcatalyst.platform.client.ClientAuthConfig;
import tech.flowcatalyst.platform.client.ClientAuthConfigRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ClientAuthConfigsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.ClientAuthConfigs.CLIENT_AUTH_CONFIGS;

/**
 * JOOQ-based implementation of ClientAuthConfigRepository.
 */
@ApplicationScoped
public class JooqClientAuthConfigRepository implements ClientAuthConfigRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<ClientAuthConfig> findByIdOptional(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(CLIENT_AUTH_CONFIGS)
                .where(CLIENT_AUTH_CONFIGS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<ClientAuthConfig> findByEmailDomain(String emailDomain) {
        return Optional.ofNullable(
            dsl.selectFrom(CLIENT_AUTH_CONFIGS)
                .where(CLIENT_AUTH_CONFIGS.EMAIL_DOMAIN.eq(emailDomain))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<ClientAuthConfig> findByClientId(String clientId) {
        return dsl.selectFrom(CLIENT_AUTH_CONFIGS)
            .where(CLIENT_AUTH_CONFIGS.CLIENT_ID.eq(clientId)
                .or(CLIENT_AUTH_CONFIGS.PRIMARY_CLIENT_ID.eq(clientId)))
            .fetch(this::toDomain);
    }

    @Override
    public List<ClientAuthConfig> findByConfigType(AuthConfigType configType) {
        return dsl.selectFrom(CLIENT_AUTH_CONFIGS)
            .where(CLIENT_AUTH_CONFIGS.CONFIG_TYPE.eq(configType.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<ClientAuthConfig> listAll() {
        return dsl.selectFrom(CLIENT_AUTH_CONFIGS)
            .fetch(this::toDomain);
    }

    @Override
    public boolean existsByEmailDomain(String emailDomain) {
        return dsl.fetchExists(
            dsl.selectFrom(CLIENT_AUTH_CONFIGS)
                .where(CLIENT_AUTH_CONFIGS.EMAIL_DOMAIN.eq(emailDomain))
        );
    }

    @Override
    public void persist(ClientAuthConfig config) {
        ClientAuthConfigsRecord record = toRecord(config);
        record.setCreatedAt(toOffsetDateTime(config.createdAt));
        record.setUpdatedAt(toOffsetDateTime(config.updatedAt));
        dsl.insertInto(CLIENT_AUTH_CONFIGS).set(record).execute();
    }

    @Override
    public void update(ClientAuthConfig config) {
        config.updatedAt = Instant.now();
        ClientAuthConfigsRecord record = toRecord(config);
        record.setUpdatedAt(toOffsetDateTime(config.updatedAt));
        dsl.update(CLIENT_AUTH_CONFIGS)
            .set(record)
            .where(CLIENT_AUTH_CONFIGS.ID.eq(config.id))
            .execute();
    }

    @Override
    public void delete(ClientAuthConfig config) {
        dsl.deleteFrom(CLIENT_AUTH_CONFIGS)
            .where(CLIENT_AUTH_CONFIGS.ID.eq(config.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private ClientAuthConfig toDomain(Record record) {
        if (record == null) return null;

        ClientAuthConfig c = new ClientAuthConfig();
        c.id = record.get(CLIENT_AUTH_CONFIGS.ID);
        c.emailDomain = record.get(CLIENT_AUTH_CONFIGS.EMAIL_DOMAIN);
        c.configType = parseEnum(record.get(CLIENT_AUTH_CONFIGS.CONFIG_TYPE), AuthConfigType.class);
        c.clientId = record.get(CLIENT_AUTH_CONFIGS.CLIENT_ID);
        c.primaryClientId = record.get(CLIENT_AUTH_CONFIGS.PRIMARY_CLIENT_ID);
        c.authProvider = parseEnum(record.get(CLIENT_AUTH_CONFIGS.AUTH_PROVIDER), AuthProvider.class);
        c.oidcIssuerUrl = record.get(CLIENT_AUTH_CONFIGS.OIDC_ISSUER_URL);
        c.oidcClientId = record.get(CLIENT_AUTH_CONFIGS.OIDC_CLIENT_ID);
        c.oidcClientSecretRef = record.get(CLIENT_AUTH_CONFIGS.OIDC_CLIENT_SECRET_REF);
        c.oidcMultiTenant = Boolean.TRUE.equals(record.get(CLIENT_AUTH_CONFIGS.OIDC_MULTI_TENANT));
        c.oidcIssuerPattern = record.get(CLIENT_AUTH_CONFIGS.OIDC_ISSUER_PATTERN);
        c.createdAt = toInstant(record.get(CLIENT_AUTH_CONFIGS.CREATED_AT));
        c.updatedAt = toInstant(record.get(CLIENT_AUTH_CONFIGS.UPDATED_AT));

        // Array columns
        c.additionalClientIds = toList(record.get(CLIENT_AUTH_CONFIGS.ADDITIONAL_CLIENT_IDS));
        c.grantedClientIds = toList(record.get(CLIENT_AUTH_CONFIGS.GRANTED_CLIENT_IDS));

        return c;
    }

    private ClientAuthConfigsRecord toRecord(ClientAuthConfig c) {
        ClientAuthConfigsRecord rec = new ClientAuthConfigsRecord();
        rec.setId(c.id);
        rec.setEmailDomain(c.emailDomain);
        rec.setConfigType(c.configType != null ? c.configType.name() : AuthConfigType.CLIENT.name());
        rec.setClientId(c.clientId);
        rec.setPrimaryClientId(c.primaryClientId);
        rec.setAuthProvider(c.authProvider != null ? c.authProvider.name() : AuthProvider.INTERNAL.name());
        rec.setOidcIssuerUrl(c.oidcIssuerUrl);
        rec.setOidcClientId(c.oidcClientId);
        rec.setOidcClientSecretRef(c.oidcClientSecretRef);
        rec.setOidcMultiTenant(c.oidcMultiTenant);
        rec.setOidcIssuerPattern(c.oidcIssuerPattern);

        // Array columns
        rec.setAdditionalClientIds(toArray(c.additionalClientIds));
        rec.setGrantedClientIds(toArray(c.grantedClientIds));

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

    private List<String> toList(Object[] array) {
        if (array == null) return new ArrayList<>();
        List<String> list = new ArrayList<>();
        for (Object obj : array) {
            if (obj != null) {
                list.add(obj.toString());
            }
        }
        return list;
    }

    private String[] toArray(List<String> list) {
        if (list == null || list.isEmpty()) {
            return new String[0];
        }
        return list.toArray(new String[0]);
    }
}
