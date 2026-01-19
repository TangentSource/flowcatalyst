package tech.flowcatalyst.platform.authorization.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authorization.AuthRole;
import tech.flowcatalyst.platform.authorization.AuthRoleRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.AuthRolesRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.AuthRoles.AUTH_ROLES;

/**
 * JOOQ-based implementation of AuthRoleRepository.
 */
@ApplicationScoped
public class JooqAuthRoleRepository implements AuthRoleRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<AuthRole> findByName(String name) {
        return Optional.ofNullable(
            dsl.selectFrom(AUTH_ROLES)
                .where(AUTH_ROLES.NAME.eq(name))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<AuthRole> findByApplicationCode(String applicationCode) {
        return dsl.selectFrom(AUTH_ROLES)
            .where(AUTH_ROLES.APPLICATION_CODE.eq(applicationCode))
            .fetch(this::toDomain);
    }

    @Override
    public List<AuthRole> findBySource(AuthRole.RoleSource source) {
        return dsl.selectFrom(AUTH_ROLES)
            .where(AUTH_ROLES.SOURCE.eq(source.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<AuthRole> listAll() {
        return dsl.selectFrom(AUTH_ROLES)
            .fetch(this::toDomain);
    }

    @Override
    public boolean existsByName(String name) {
        return dsl.fetchExists(
            dsl.selectFrom(AUTH_ROLES)
                .where(AUTH_ROLES.NAME.eq(name))
        );
    }

    @Override
    public void persist(AuthRole role) {
        AuthRolesRecord record = toRecord(role);
        record.setCreatedAt(toOffsetDateTime(role.createdAt));
        record.setUpdatedAt(toOffsetDateTime(role.updatedAt));
        dsl.insertInto(AUTH_ROLES).set(record).execute();
    }

    @Override
    public void update(AuthRole role) {
        role.updatedAt = Instant.now();
        AuthRolesRecord record = toRecord(role);
        record.setUpdatedAt(toOffsetDateTime(role.updatedAt));
        dsl.update(AUTH_ROLES)
            .set(record)
            .where(AUTH_ROLES.ID.eq(role.id))
            .execute();
    }

    @Override
    public void delete(AuthRole role) {
        dsl.deleteFrom(AUTH_ROLES)
            .where(AUTH_ROLES.ID.eq(role.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private AuthRole toDomain(Record record) {
        if (record == null) return null;

        AuthRole r = new AuthRole();
        r.id = record.get(AUTH_ROLES.ID);
        r.applicationId = record.get(AUTH_ROLES.APPLICATION_ID);
        r.applicationCode = record.get(AUTH_ROLES.APPLICATION_CODE);
        r.name = record.get(AUTH_ROLES.NAME);
        r.displayName = record.get(AUTH_ROLES.DISPLAY_NAME);
        r.description = record.get(AUTH_ROLES.DESCRIPTION);
        r.source = parseEnum(record.get(AUTH_ROLES.SOURCE), AuthRole.RoleSource.class);
        r.clientManaged = record.get(AUTH_ROLES.CLIENT_MANAGED);
        r.createdAt = toInstant(record.get(AUTH_ROLES.CREATED_AT));
        r.updatedAt = toInstant(record.get(AUTH_ROLES.UPDATED_AT));

        // Permissions (TEXT[] array)
        Object[] permArray = record.get(AUTH_ROLES.PERMISSIONS);
        if (permArray != null) {
            r.permissions = new HashSet<>();
            for (Object obj : permArray) {
                if (obj != null) {
                    r.permissions.add(obj.toString());
                }
            }
        }

        return r;
    }

    private AuthRolesRecord toRecord(AuthRole r) {
        AuthRolesRecord rec = new AuthRolesRecord();
        rec.setId(r.id);
        rec.setApplicationId(r.applicationId);
        rec.setApplicationCode(r.applicationCode);
        rec.setName(r.name);
        rec.setDisplayName(r.displayName);
        rec.setDescription(r.description);
        rec.setSource(r.source != null ? r.source.name() : AuthRole.RoleSource.DATABASE.name());
        rec.setClientManaged(r.clientManaged);

        // Convert Set<String> to String[]
        if (r.permissions != null && !r.permissions.isEmpty()) {
            rec.setPermissions(r.permissions.toArray(new String[0]));
        } else {
            rec.setPermissions(new String[0]);
        }

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
}
