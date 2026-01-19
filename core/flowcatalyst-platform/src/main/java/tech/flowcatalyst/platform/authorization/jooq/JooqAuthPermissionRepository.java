package tech.flowcatalyst.platform.authorization.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authorization.AuthPermission;
import tech.flowcatalyst.platform.authorization.AuthPermissionRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.AuthPermissionsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.AuthPermissions.AUTH_PERMISSIONS;

/**
 * JOOQ-based implementation of AuthPermissionRepository.
 */
@ApplicationScoped
public class JooqAuthPermissionRepository implements AuthPermissionRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<AuthPermission> findByName(String name) {
        return Optional.ofNullable(
            dsl.selectFrom(AUTH_PERMISSIONS)
                .where(AUTH_PERMISSIONS.NAME.eq(name))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<AuthPermission> findByApplicationId(String applicationId) {
        return dsl.selectFrom(AUTH_PERMISSIONS)
            .where(AUTH_PERMISSIONS.APPLICATION_ID.eq(applicationId))
            .fetch(this::toDomain);
    }

    @Override
    public List<AuthPermission> listAll() {
        return dsl.selectFrom(AUTH_PERMISSIONS)
            .fetch(this::toDomain);
    }

    @Override
    public boolean existsByName(String name) {
        return dsl.fetchExists(
            dsl.selectFrom(AUTH_PERMISSIONS)
                .where(AUTH_PERMISSIONS.NAME.eq(name))
        );
    }

    @Override
    public void persist(AuthPermission permission) {
        AuthPermissionsRecord record = toRecord(permission);
        record.setCreatedAt(toOffsetDateTime(permission.createdAt));
        dsl.insertInto(AUTH_PERMISSIONS).set(record).execute();
    }

    @Override
    public void update(AuthPermission permission) {
        AuthPermissionsRecord record = toRecord(permission);
        dsl.update(AUTH_PERMISSIONS)
            .set(record)
            .where(AUTH_PERMISSIONS.ID.eq(permission.id))
            .execute();
    }

    @Override
    public void delete(AuthPermission permission) {
        dsl.deleteFrom(AUTH_PERMISSIONS)
            .where(AUTH_PERMISSIONS.ID.eq(permission.id))
            .execute();
    }

    @Override
    public long deleteByApplicationId(String applicationId) {
        return dsl.deleteFrom(AUTH_PERMISSIONS)
            .where(AUTH_PERMISSIONS.APPLICATION_ID.eq(applicationId))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private AuthPermission toDomain(Record record) {
        if (record == null) return null;

        AuthPermission p = new AuthPermission();
        p.id = record.get(AUTH_PERMISSIONS.ID);
        p.applicationId = record.get(AUTH_PERMISSIONS.APPLICATION_ID);
        p.name = record.get(AUTH_PERMISSIONS.NAME);
        p.displayName = record.get(AUTH_PERMISSIONS.DISPLAY_NAME);
        p.description = record.get(AUTH_PERMISSIONS.DESCRIPTION);
        p.source = parseEnum(record.get(AUTH_PERMISSIONS.SOURCE), AuthPermission.PermissionSource.class);
        p.createdAt = toInstant(record.get(AUTH_PERMISSIONS.CREATED_AT));
        return p;
    }

    private AuthPermissionsRecord toRecord(AuthPermission p) {
        AuthPermissionsRecord rec = new AuthPermissionsRecord();
        rec.setId(p.id);
        rec.setApplicationId(p.applicationId);
        rec.setName(p.name);
        rec.setDisplayName(p.displayName);
        rec.setDescription(p.description);
        rec.setSource(p.source != null ? p.source.name() : AuthPermission.PermissionSource.SDK.name());
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
