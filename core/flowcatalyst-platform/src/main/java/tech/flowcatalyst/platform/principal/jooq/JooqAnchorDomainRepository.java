package tech.flowcatalyst.platform.principal.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.jooq.generated.tables.records.AnchorDomainsRecord;
import tech.flowcatalyst.platform.principal.AnchorDomain;
import tech.flowcatalyst.platform.principal.AnchorDomainRepository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.AnchorDomains.ANCHOR_DOMAINS;

/**
 * JOOQ-based implementation of AnchorDomainRepository.
 */
@ApplicationScoped
public class JooqAnchorDomainRepository implements AnchorDomainRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<AnchorDomain> findByIdOptional(String id) {
        return Optional.ofNullable(
            dsl.selectFrom(ANCHOR_DOMAINS)
                .where(ANCHOR_DOMAINS.ID.eq(id))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<AnchorDomain> listAll() {
        return dsl.selectFrom(ANCHOR_DOMAINS)
            .fetch(this::toDomain);
    }

    @Override
    public boolean existsByDomain(String domain) {
        return dsl.fetchExists(
            dsl.selectFrom(ANCHOR_DOMAINS)
                .where(ANCHOR_DOMAINS.DOMAIN.eq(domain))
        );
    }

    @Override
    public void persist(AnchorDomain domain) {
        AnchorDomainsRecord record = new AnchorDomainsRecord();
        record.setId(domain.id);
        record.setDomain(domain.domain);
        record.setCreatedAt(toOffsetDateTime(domain.createdAt));
        dsl.insertInto(ANCHOR_DOMAINS).set(record).execute();
    }

    @Override
    public void delete(AnchorDomain domain) {
        dsl.deleteFrom(ANCHOR_DOMAINS)
            .where(ANCHOR_DOMAINS.ID.eq(domain.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private AnchorDomain toDomain(Record record) {
        if (record == null) return null;

        AnchorDomain ad = new AnchorDomain();
        ad.id = record.get(ANCHOR_DOMAINS.ID);
        ad.domain = record.get(ANCHOR_DOMAINS.DOMAIN);
        ad.createdAt = toInstant(record.get(ANCHOR_DOMAINS.CREATED_AT));
        return ad;
    }

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }
}
