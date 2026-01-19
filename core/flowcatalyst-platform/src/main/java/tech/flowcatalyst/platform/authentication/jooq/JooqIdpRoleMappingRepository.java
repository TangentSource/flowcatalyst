package tech.flowcatalyst.platform.authentication.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.IdpRoleMapping;
import tech.flowcatalyst.platform.authentication.IdpRoleMappingRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.IdpRoleMappingsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.IdpRoleMappings.IDP_ROLE_MAPPINGS;

/**
 * JOOQ-based implementation of IdpRoleMappingRepository.
 */
@ApplicationScoped
public class JooqIdpRoleMappingRepository implements IdpRoleMappingRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<IdpRoleMapping> findByIdpRoleName(String idpRoleName) {
        return Optional.ofNullable(
            dsl.selectFrom(IDP_ROLE_MAPPINGS)
                .where(IDP_ROLE_MAPPINGS.IDP_ROLE_NAME.eq(idpRoleName))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public void persist(IdpRoleMapping mapping) {
        IdpRoleMappingsRecord record = toRecord(mapping);
        record.setCreatedAt(toOffsetDateTime(mapping.createdAt));
        dsl.insertInto(IDP_ROLE_MAPPINGS).set(record).execute();
    }

    @Override
    public void delete(IdpRoleMapping mapping) {
        dsl.deleteFrom(IDP_ROLE_MAPPINGS)
            .where(IDP_ROLE_MAPPINGS.ID.eq(mapping.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private IdpRoleMapping toDomain(Record record) {
        if (record == null) return null;

        IdpRoleMapping m = new IdpRoleMapping();
        m.id = record.get(IDP_ROLE_MAPPINGS.ID);
        m.idpRoleName = record.get(IDP_ROLE_MAPPINGS.IDP_ROLE_NAME);
        m.internalRoleName = record.get(IDP_ROLE_MAPPINGS.INTERNAL_ROLE_NAME);
        m.createdAt = toInstant(record.get(IDP_ROLE_MAPPINGS.CREATED_AT));
        return m;
    }

    private IdpRoleMappingsRecord toRecord(IdpRoleMapping m) {
        IdpRoleMappingsRecord rec = new IdpRoleMappingsRecord();
        rec.setId(m.id);
        rec.setIdpRoleName(m.idpRoleName);
        rec.setInternalRoleName(m.internalRoleName);
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
}
