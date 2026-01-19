package tech.flowcatalyst.platform.application.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.application.Application;
import tech.flowcatalyst.platform.application.ApplicationRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ApplicationsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.Applications.APPLICATIONS;

/**
 * JOOQ-based implementation of ApplicationRepository.
 */
@ApplicationScoped
public class JooqApplicationRepository implements ApplicationRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<Application> findByIdOptional(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(APPLICATIONS)
                .where(APPLICATIONS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<Application> findByCode(String code) {
        return Optional.ofNullable(
            dsl.selectFrom(APPLICATIONS)
                .where(APPLICATIONS.CODE.eq(code))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Application> findAllActive() {
        return dsl.selectFrom(APPLICATIONS)
            .where(APPLICATIONS.ACTIVE.eq(true))
            .fetch(this::toDomain);
    }

    @Override
    public List<Application> findByType(Application.ApplicationType type, boolean activeOnly) {
        var query = dsl.selectFrom(APPLICATIONS)
            .where(APPLICATIONS.TYPE.eq(type.name()));
        if (activeOnly) {
            query = query.and(APPLICATIONS.ACTIVE.eq(true));
        }
        return query.fetch(this::toDomain);
    }

    @Override
    public List<Application> findByCodes(Collection<String> codes) {
        if (codes == null || codes.isEmpty()) {
            return new ArrayList<>();
        }
        return dsl.selectFrom(APPLICATIONS)
            .where(APPLICATIONS.CODE.in(codes))
            .fetch(this::toDomain);
    }

    @Override
    public List<Application> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return dsl.selectFrom(APPLICATIONS)
            .where(APPLICATIONS.ID.in(ids))
            .fetch(this::toDomain);
    }

    @Override
    public List<Application> listAll() {
        return dsl.selectFrom(APPLICATIONS)
            .fetch(this::toDomain);
    }

    @Override
    public boolean existsByCode(String code) {
        return dsl.fetchExists(
            dsl.selectFrom(APPLICATIONS)
                .where(APPLICATIONS.CODE.eq(code))
        );
    }

    @Override
    public void persist(Application application) {
        ApplicationsRecord record = toRecord(application);
        record.setCreatedAt(toOffsetDateTime(application.createdAt));
        record.setUpdatedAt(toOffsetDateTime(application.updatedAt));
        dsl.insertInto(APPLICATIONS).set(record).execute();
    }

    @Override
    public void update(Application application) {
        application.updatedAt = Instant.now();
        ApplicationsRecord record = toRecord(application);
        record.setUpdatedAt(toOffsetDateTime(application.updatedAt));
        dsl.update(APPLICATIONS)
            .set(record)
            .where(APPLICATIONS.ID.eq(application.id))
            .execute();
    }

    @Override
    public void delete(Application application) {
        dsl.deleteFrom(APPLICATIONS)
            .where(APPLICATIONS.ID.eq(application.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Application toDomain(Record record) {
        if (record == null) return null;

        Application a = new Application();
        a.id = record.get(APPLICATIONS.ID);
        a.type = parseEnum(record.get(APPLICATIONS.TYPE), Application.ApplicationType.class);
        a.code = record.get(APPLICATIONS.CODE);
        a.name = record.get(APPLICATIONS.NAME);
        a.description = record.get(APPLICATIONS.DESCRIPTION);
        a.iconUrl = record.get(APPLICATIONS.ICON_URL);
        a.website = record.get(APPLICATIONS.WEBSITE);
        a.logo = record.get(APPLICATIONS.LOGO);
        a.logoMimeType = record.get(APPLICATIONS.LOGO_MIME_TYPE);
        a.defaultBaseUrl = record.get(APPLICATIONS.DEFAULT_BASE_URL);
        a.serviceAccountId = record.get(APPLICATIONS.SERVICE_ACCOUNT_ID);
        a.active = record.get(APPLICATIONS.ACTIVE);
        a.createdAt = toInstant(record.get(APPLICATIONS.CREATED_AT));
        a.updatedAt = toInstant(record.get(APPLICATIONS.UPDATED_AT));
        return a;
    }

    private ApplicationsRecord toRecord(Application a) {
        ApplicationsRecord rec = new ApplicationsRecord();
        rec.setId(a.id);
        rec.setType(a.type != null ? a.type.name() : Application.ApplicationType.APPLICATION.name());
        rec.setCode(a.code);
        rec.setName(a.name);
        rec.setDescription(a.description);
        rec.setIconUrl(a.iconUrl);
        rec.setWebsite(a.website);
        rec.setLogo(a.logo);
        rec.setLogoMimeType(a.logoMimeType);
        rec.setDefaultBaseUrl(a.defaultBaseUrl);
        rec.setServiceAccountId(a.serviceAccountId);
        rec.setActive(a.active);
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
