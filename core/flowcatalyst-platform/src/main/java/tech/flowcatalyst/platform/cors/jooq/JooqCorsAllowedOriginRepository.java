package tech.flowcatalyst.platform.cors.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.cors.CorsAllowedOrigin;
import tech.flowcatalyst.platform.jooq.generated.tables.records.CorsAllowedOriginsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.CorsAllowedOrigins.CORS_ALLOWED_ORIGINS;

/**
 * JOOQ-based implementation of CORS allowed origins repository.
 *
 * <p>This is an alternative to the JDBI-based CorsAllowedOriginRepository.
 * Enable via beans.xml or by removing the JDBI version.
 */
@ApplicationScoped
public class JooqCorsAllowedOriginRepository {

    @Inject
    DSLContext dsl;

    public Optional<CorsAllowedOrigin> findById(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(CORS_ALLOWED_ORIGINS)
                .where(CORS_ALLOWED_ORIGINS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    public Optional<CorsAllowedOrigin> findByOrigin(String origin) {
        return Optional.ofNullable(
            dsl.selectFrom(CORS_ALLOWED_ORIGINS)
                .where(CORS_ALLOWED_ORIGINS.ORIGIN.eq(origin))
                .fetchOne(this::toDomain)
        );
    }

    public List<CorsAllowedOrigin> listAll() {
        return dsl.selectFrom(CORS_ALLOWED_ORIGINS)
            .orderBy(CORS_ALLOWED_ORIGINS.ORIGIN)
            .fetch(this::toDomain);
    }

    public boolean existsByOrigin(String origin) {
        return dsl.fetchExists(
            dsl.selectFrom(CORS_ALLOWED_ORIGINS)
                .where(CORS_ALLOWED_ORIGINS.ORIGIN.eq(origin))
        );
    }

    public long count() {
        return dsl.selectCount()
            .from(CORS_ALLOWED_ORIGINS)
            .fetchOne(0, Long.class);
    }

    public void persist(CorsAllowedOrigin entry) {
        CorsAllowedOriginsRecord record = toRecord(entry);
        record.setCreatedAt(toOffsetDateTime(entry.createdAt));
        dsl.insertInto(CORS_ALLOWED_ORIGINS).set(record).execute();
    }

    public void delete(CorsAllowedOrigin entry) {
        deleteById(entry.id);
    }

    public boolean deleteById(String id) {
        return dsl.deleteFrom(CORS_ALLOWED_ORIGINS)
            .where(CORS_ALLOWED_ORIGINS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private CorsAllowedOrigin toDomain(Record record) {
        if (record == null) return null;

        CorsAllowedOrigin entry = new CorsAllowedOrigin();
        entry.id = record.get(CORS_ALLOWED_ORIGINS.ID);
        entry.origin = record.get(CORS_ALLOWED_ORIGINS.ORIGIN);
        entry.description = record.get(CORS_ALLOWED_ORIGINS.DESCRIPTION);
        entry.createdBy = record.get(CORS_ALLOWED_ORIGINS.CREATED_BY);
        entry.createdAt = toInstant(record.get(CORS_ALLOWED_ORIGINS.CREATED_AT));

        return entry;
    }

    private CorsAllowedOriginsRecord toRecord(CorsAllowedOrigin entry) {
        CorsAllowedOriginsRecord rec = new CorsAllowedOriginsRecord();
        rec.setId(entry.id);
        rec.setOrigin(entry.origin);
        rec.setDescription(entry.description);
        rec.setCreatedBy(entry.createdBy);
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
