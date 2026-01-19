package tech.flowcatalyst.platform.principal.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.IdpType;
import tech.flowcatalyst.platform.jooq.generated.tables.records.PrincipalsRecord;
import tech.flowcatalyst.platform.principal.*;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.Principals.PRINCIPALS;

/**
 * JOOQ-based implementation of PrincipalRepository.
 *
 * <p>This is an alternative implementation that uses JOOQ's type-safe DSL
 * instead of JDBI SQL strings. Enable it by adding to beans.xml:
 * <pre>{@code
 * <alternatives>
 *   <class>tech.flowcatalyst.platform.principal.jooq.JooqPrincipalRepository</class>
 * </alternatives>
 * }</pre>
 */
@ApplicationScoped
public class JooqPrincipalRepository implements PrincipalRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public Principal findById(String id) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<Principal> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<Principal> findByEmail(String email) {
        return Optional.ofNullable(
            dsl.selectFrom(PRINCIPALS)
                .where(PRINCIPALS.EMAIL.eq(email))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<Principal> findByServiceAccountCode(String code) {
        // JSONB query: service_account->>'code' = :code
        return Optional.ofNullable(
            dsl.selectFrom(PRINCIPALS)
                .where(org.jooq.impl.DSL.field(
                    "service_account->>'code'", String.class
                ).eq(code))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Principal> findByType(PrincipalType type) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.TYPE.eq(type.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByClientId(String clientId) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.ID.in(ids))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findUsersByClientId(String clientId) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .and(PRINCIPALS.TYPE.eq(PrincipalType.USER.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findActiveUsersByClientId(String clientId) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .and(PRINCIPALS.TYPE.eq(PrincipalType.USER.name()))
            .and(PRINCIPALS.ACTIVE.eq(true))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByClientIdAndTypeAndActive(String clientId, PrincipalType type, Boolean active) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .and(PRINCIPALS.TYPE.eq(type.name()))
            .and(PRINCIPALS.ACTIVE.eq(active))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByClientIdAndType(String clientId, PrincipalType type) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .and(PRINCIPALS.TYPE.eq(type.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByClientIdAndActive(String clientId, Boolean active) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.CLIENT_ID.eq(clientId))
            .and(PRINCIPALS.ACTIVE.eq(active))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> findByActive(Boolean active) {
        return dsl.selectFrom(PRINCIPALS)
            .where(PRINCIPALS.ACTIVE.eq(active))
            .fetch(this::toDomain);
    }

    @Override
    public List<Principal> listAll() {
        return dsl.selectFrom(PRINCIPALS)
            .fetch(this::toDomain);
    }

    @Override
    public Optional<Principal> findByServiceAccountClientId(String clientId) {
        // JSONB query: type = 'SERVICE' AND service_account->>'clientId' = :clientId
        return Optional.ofNullable(
            dsl.selectFrom(PRINCIPALS)
                .where(PRINCIPALS.TYPE.eq(PrincipalType.SERVICE.name()))
                .and(org.jooq.impl.DSL.field(
                    "service_account->>'clientId'", String.class
                ).eq(clientId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public long countByEmailDomain(String domain) {
        return dsl.selectCount()
            .from(PRINCIPALS)
            .where(PRINCIPALS.EMAIL_DOMAIN.eq(domain))
            .fetchOne(0, Long.class);
    }

    @Override
    public void persist(Principal principal) {
        PrincipalsRecord record = toRecord(principal);
        record.setCreatedAt(toOffsetDateTime(principal.createdAt));
        record.setUpdatedAt(toOffsetDateTime(principal.updatedAt));
        dsl.insertInto(PRINCIPALS).set(record).execute();
    }

    @Override
    public void update(Principal principal) {
        principal.updatedAt = Instant.now();
        PrincipalsRecord record = toRecord(principal);
        record.setUpdatedAt(toOffsetDateTime(principal.updatedAt));
        dsl.update(PRINCIPALS)
            .set(record)
            .where(PRINCIPALS.ID.eq(principal.id))
            .execute();
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(PRINCIPALS)
            .where(PRINCIPALS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Principal toDomain(Record record) {
        if (record == null) return null;

        Principal p = new Principal();
        p.id = record.get(PRINCIPALS.ID);
        p.type = parseEnum(record.get(PRINCIPALS.TYPE), PrincipalType.class);
        p.scope = parseEnum(record.get(PRINCIPALS.SCOPE), UserScope.class);
        p.clientId = record.get(PRINCIPALS.CLIENT_ID);
        p.applicationId = record.get(PRINCIPALS.APPLICATION_ID);
        p.name = record.get(PRINCIPALS.NAME);
        p.active = record.get(PRINCIPALS.ACTIVE);
        p.createdAt = toInstant(record.get(PRINCIPALS.CREATED_AT));
        p.updatedAt = toInstant(record.get(PRINCIPALS.UPDATED_AT));

        // UserIdentity (embedded from flat columns)
        String email = record.get(PRINCIPALS.EMAIL);
        if (email != null) {
            UserIdentity ui = new UserIdentity();
            ui.email = email;
            ui.emailDomain = record.get(PRINCIPALS.EMAIL_DOMAIN);
            ui.idpType = parseEnum(record.get(PRINCIPALS.IDP_TYPE), IdpType.class);
            ui.externalIdpId = record.get(PRINCIPALS.EXTERNAL_IDP_ID);
            ui.passwordHash = record.get(PRINCIPALS.PASSWORD_HASH);
            ui.lastLoginAt = toInstant(record.get(PRINCIPALS.LAST_LOGIN_AT));
            p.userIdentity = ui;
        }

        // ServiceAccount (JSONB)
        String saJson = record.get(PRINCIPALS.SERVICE_ACCOUNT);
        if (saJson != null && !saJson.isBlank()) {
            p.serviceAccount = parseJson(saJson, ServiceAccount.class);
        }

        // Roles (JSONB array)
        String rolesJson = record.get(PRINCIPALS.ROLES);
        if (rolesJson != null && !rolesJson.isBlank()) {
            p.roles = parseJson(rolesJson, new TypeReference<List<Principal.RoleAssignment>>() {});
            if (p.roles == null) {
                p.roles = new ArrayList<>();
            }
        }

        return p;
    }

    private PrincipalsRecord toRecord(Principal p) {
        PrincipalsRecord r = new PrincipalsRecord();
        r.setId(p.id);
        r.setType(p.type != null ? p.type.name() : null);
        r.setScope(p.scope != null ? p.scope.name() : null);
        r.setClientId(p.clientId);
        r.setApplicationId(p.applicationId);
        r.setName(p.name);
        r.setActive(p.active);

        // UserIdentity -> flat columns
        if (p.userIdentity != null) {
            r.setEmail(p.userIdentity.email);
            r.setEmailDomain(p.userIdentity.emailDomain);
            r.setIdpType(p.userIdentity.idpType != null ? p.userIdentity.idpType.name() : null);
            r.setExternalIdpId(p.userIdentity.externalIdpId);
            r.setPasswordHash(p.userIdentity.passwordHash);
            r.setLastLoginAt(toOffsetDateTime(p.userIdentity.lastLoginAt));
        }

        // ServiceAccount -> JSONB
        r.setServiceAccount(toJson(p.serviceAccount));

        // Roles -> JSONB array
        r.setRoles(toJson(p.roles != null ? p.roles : new ArrayList<>()));

        return r;
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

    private <T> T parseJson(String json, Class<T> clazz) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            return null;
        }
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
