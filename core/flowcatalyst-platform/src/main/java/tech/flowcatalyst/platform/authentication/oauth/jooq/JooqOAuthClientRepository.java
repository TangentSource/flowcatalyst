package tech.flowcatalyst.platform.authentication.oauth.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.platform.authentication.oauth.OAuthClient;
import tech.flowcatalyst.platform.authentication.oauth.OAuthClientRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.OauthClientsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.OauthClients.OAUTH_CLIENTS;

/**
 * JOOQ-based implementation of OAuthClientRepository.
 */
@ApplicationScoped
public class JooqOAuthClientRepository implements OAuthClientRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<OAuthClient> findByIdOptional(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(OAUTH_CLIENTS)
                .where(OAUTH_CLIENTS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<OAuthClient> findByClientId(String clientId) {
        return Optional.ofNullable(
            dsl.selectFrom(OAUTH_CLIENTS)
                .where(OAUTH_CLIENTS.CLIENT_ID.eq(clientId))
                .and(OAUTH_CLIENTS.ACTIVE.eq(true))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<OAuthClient> findByClientIdIncludingInactive(String clientId) {
        return Optional.ofNullable(
            dsl.selectFrom(OAUTH_CLIENTS)
                .where(OAUTH_CLIENTS.CLIENT_ID.eq(clientId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<OAuthClient> findByApplicationIdAndActive(String applicationId, boolean active) {
        // Check if applicationId is in the application_ids array
        return dsl.selectFrom(OAUTH_CLIENTS)
            .where(DSL.condition("{0} = ANY({1})", applicationId, OAUTH_CLIENTS.APPLICATION_IDS))
            .and(OAUTH_CLIENTS.ACTIVE.eq(active))
            .fetch(this::toDomain);
    }

    @Override
    public List<OAuthClient> findByApplicationId(String applicationId) {
        return dsl.selectFrom(OAUTH_CLIENTS)
            .where(DSL.condition("{0} = ANY({1})", applicationId, OAUTH_CLIENTS.APPLICATION_IDS))
            .fetch(this::toDomain);
    }

    @Override
    public List<OAuthClient> findByActive(boolean active) {
        return dsl.selectFrom(OAUTH_CLIENTS)
            .where(OAUTH_CLIENTS.ACTIVE.eq(active))
            .fetch(this::toDomain);
    }

    @Override
    public List<OAuthClient> listAll() {
        return dsl.selectFrom(OAUTH_CLIENTS)
            .fetch(this::toDomain);
    }

    @Override
    public boolean isOriginAllowedByAnyClient(String origin) {
        return dsl.fetchExists(
            dsl.selectFrom(OAUTH_CLIENTS)
                .where(DSL.condition("{0} = ANY({1})", origin, OAUTH_CLIENTS.ALLOWED_ORIGINS))
                .and(OAUTH_CLIENTS.ACTIVE.eq(true))
        );
    }

    @Override
    public boolean isOriginUsedByAnyClient(String origin) {
        // Check allowed_origins and redirect_uris (redirect URIs starting with origin)
        String originPattern = origin + "%";
        return dsl.fetchExists(
            dsl.selectFrom(OAUTH_CLIENTS)
                .where(DSL.condition("{0} = ANY({1})", origin, OAUTH_CLIENTS.ALLOWED_ORIGINS))
                .or(DSL.exists(
                    dsl.selectOne()
                        .from(DSL.unnest(OAUTH_CLIENTS.REDIRECT_URIS).as("uri"))
                        .where(DSL.field("uri", String.class).like(originPattern))
                ))
        );
    }

    @Override
    public List<String> findClientNamesUsingOrigin(String origin) {
        String originPattern = origin + "%";
        return dsl.select(OAUTH_CLIENTS.CLIENT_NAME)
            .from(OAUTH_CLIENTS)
            .where(DSL.condition("{0} = ANY({1})", origin, OAUTH_CLIENTS.ALLOWED_ORIGINS))
            .or(DSL.exists(
                dsl.selectOne()
                    .from(DSL.unnest(OAUTH_CLIENTS.REDIRECT_URIS).as("uri"))
                    .where(DSL.field("uri", String.class).like(originPattern))
            ))
            .fetch(OAUTH_CLIENTS.CLIENT_NAME);
    }

    @Override
    public void persist(OAuthClient client) {
        OauthClientsRecord record = toRecord(client);
        record.setCreatedAt(toOffsetDateTime(client.createdAt));
        record.setUpdatedAt(toOffsetDateTime(client.updatedAt));
        dsl.insertInto(OAUTH_CLIENTS).set(record).execute();
    }

    @Override
    public void update(OAuthClient client) {
        client.updatedAt = Instant.now();
        OauthClientsRecord record = toRecord(client);
        record.setUpdatedAt(toOffsetDateTime(client.updatedAt));
        dsl.update(OAUTH_CLIENTS)
            .set(record)
            .where(OAUTH_CLIENTS.ID.eq(client.id))
            .execute();
    }

    @Override
    public void delete(OAuthClient client) {
        dsl.deleteFrom(OAUTH_CLIENTS)
            .where(OAUTH_CLIENTS.ID.eq(client.id))
            .execute();
    }

    @Override
    public long deleteByServiceAccountPrincipalId(String principalId) {
        return dsl.deleteFrom(OAUTH_CLIENTS)
            .where(OAUTH_CLIENTS.SERVICE_ACCOUNT_PRINCIPAL_ID.eq(principalId))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private OAuthClient toDomain(Record record) {
        if (record == null) return null;

        OAuthClient c = new OAuthClient();
        c.id = record.get(OAUTH_CLIENTS.ID);
        c.clientId = record.get(OAUTH_CLIENTS.CLIENT_ID);
        c.clientName = record.get(OAUTH_CLIENTS.CLIENT_NAME);
        c.clientType = parseEnum(record.get(OAUTH_CLIENTS.CLIENT_TYPE), OAuthClient.ClientType.class);
        c.clientSecretRef = record.get(OAUTH_CLIENTS.CLIENT_SECRET_REF);
        c.defaultScopes = record.get(OAUTH_CLIENTS.DEFAULT_SCOPES);
        c.pkceRequired = record.get(OAUTH_CLIENTS.PKCE_REQUIRED);
        c.serviceAccountPrincipalId = record.get(OAUTH_CLIENTS.SERVICE_ACCOUNT_PRINCIPAL_ID);
        c.active = record.get(OAUTH_CLIENTS.ACTIVE);
        c.createdAt = toInstant(record.get(OAUTH_CLIENTS.CREATED_AT));
        c.updatedAt = toInstant(record.get(OAUTH_CLIENTS.UPDATED_AT));

        // Array columns
        c.redirectUris = toList(record.get(OAUTH_CLIENTS.REDIRECT_URIS));
        c.allowedOrigins = toList(record.get(OAUTH_CLIENTS.ALLOWED_ORIGINS));
        c.grantTypes = toList(record.get(OAUTH_CLIENTS.GRANT_TYPES));
        c.applicationIds = toList(record.get(OAUTH_CLIENTS.APPLICATION_IDS));

        return c;
    }

    private OauthClientsRecord toRecord(OAuthClient c) {
        OauthClientsRecord rec = new OauthClientsRecord();
        rec.setId(c.id);
        rec.setClientId(c.clientId);
        rec.setClientName(c.clientName);
        rec.setClientType(c.clientType != null ? c.clientType.name() : OAuthClient.ClientType.PUBLIC.name());
        rec.setClientSecretRef(c.clientSecretRef);
        rec.setDefaultScopes(c.defaultScopes);
        rec.setPkceRequired(c.pkceRequired);
        rec.setServiceAccountPrincipalId(c.serviceAccountPrincipalId);
        rec.setActive(c.active);

        // Array columns
        rec.setRedirectUris(toArray(c.redirectUris));
        rec.setAllowedOrigins(toArray(c.allowedOrigins));
        rec.setGrantTypes(toArray(c.grantTypes));
        rec.setApplicationIds(toArray(c.applicationIds));

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
