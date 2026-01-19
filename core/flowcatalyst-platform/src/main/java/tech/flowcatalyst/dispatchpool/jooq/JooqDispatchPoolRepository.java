package tech.flowcatalyst.dispatchpool.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.dispatchpool.DispatchPool;
import tech.flowcatalyst.dispatchpool.DispatchPoolRepository;
import tech.flowcatalyst.dispatchpool.DispatchPoolStatus;
import tech.flowcatalyst.platform.jooq.generated.tables.records.DispatchPoolsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.DispatchPools.DISPATCH_POOLS;

/**
 * JOOQ-based implementation of DispatchPoolRepository.
 */
@ApplicationScoped
public class JooqDispatchPoolRepository implements DispatchPoolRepository {

    @Inject
    DSLContext dsl;

    @Override
    public DispatchPool findById(String id) {
        return dsl.selectFrom(DISPATCH_POOLS)
            .where(DISPATCH_POOLS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<DispatchPool> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<DispatchPool> findByCodeAndClientId(String code, String clientId) {
        Condition condition = DISPATCH_POOLS.CODE.eq(code);
        if (clientId == null) {
            condition = condition.and(DISPATCH_POOLS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(DISPATCH_POOLS.CLIENT_ID.eq(clientId));
        }
        return Optional.ofNullable(
            dsl.selectFrom(DISPATCH_POOLS)
                .where(condition)
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<DispatchPool> findByClientId(String clientId) {
        Condition condition = clientId == null
            ? DISPATCH_POOLS.CLIENT_ID.isNull()
            : DISPATCH_POOLS.CLIENT_ID.eq(clientId);
        return dsl.selectFrom(DISPATCH_POOLS)
            .where(condition)
            .orderBy(DISPATCH_POOLS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchPool> findAnchorLevel() {
        return dsl.selectFrom(DISPATCH_POOLS)
            .where(DISPATCH_POOLS.CLIENT_ID.isNull())
            .orderBy(DISPATCH_POOLS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchPool> findByStatus(DispatchPoolStatus status) {
        return dsl.selectFrom(DISPATCH_POOLS)
            .where(DISPATCH_POOLS.STATUS.eq(status.name()))
            .orderBy(DISPATCH_POOLS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchPool> findActive() {
        return findByStatus(DispatchPoolStatus.ACTIVE);
    }

    @Override
    public List<DispatchPool> findAllNonArchived() {
        return dsl.selectFrom(DISPATCH_POOLS)
            .where(DISPATCH_POOLS.STATUS.ne(DispatchPoolStatus.ARCHIVED.name()))
            .orderBy(DISPATCH_POOLS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchPool> findWithFilters(String clientId, DispatchPoolStatus status, boolean includeArchived) {
        Condition condition = DSL.noCondition();

        if (clientId != null) {
            condition = condition.and(DISPATCH_POOLS.CLIENT_ID.eq(clientId));
        }
        if (status != null) {
            condition = condition.and(DISPATCH_POOLS.STATUS.eq(status.name()));
        }
        if (!includeArchived && status == null) {
            condition = condition.and(DISPATCH_POOLS.STATUS.ne(DispatchPoolStatus.ARCHIVED.name()));
        }

        return dsl.selectFrom(DISPATCH_POOLS)
            .where(condition)
            .orderBy(DISPATCH_POOLS.CODE)
            .fetch(this::toDomain);
    }

    @Override
    public List<DispatchPool> listAll() {
        return dsl.selectFrom(DISPATCH_POOLS)
            .fetch(this::toDomain);
    }

    @Override
    public long count() {
        return dsl.selectCount()
            .from(DISPATCH_POOLS)
            .fetchOne(0, Long.class);
    }

    @Override
    public boolean existsByCodeAndClientId(String code, String clientId) {
        Condition condition = DISPATCH_POOLS.CODE.eq(code);
        if (clientId == null) {
            condition = condition.and(DISPATCH_POOLS.CLIENT_ID.isNull());
        } else {
            condition = condition.and(DISPATCH_POOLS.CLIENT_ID.eq(clientId));
        }
        return dsl.fetchExists(
            dsl.selectFrom(DISPATCH_POOLS).where(condition)
        );
    }

    @Override
    public void persist(DispatchPool pool) {
        DispatchPoolsRecord record = toRecord(pool);
        record.setCreatedAt(toOffsetDateTime(pool.createdAt()));
        record.setUpdatedAt(toOffsetDateTime(pool.updatedAt()));
        dsl.insertInto(DISPATCH_POOLS).set(record).execute();
    }

    @Override
    public void update(DispatchPool pool) {
        DispatchPoolsRecord record = toRecord(pool);
        record.setUpdatedAt(toOffsetDateTime(Instant.now()));
        dsl.update(DISPATCH_POOLS)
            .set(record)
            .where(DISPATCH_POOLS.ID.eq(pool.id()))
            .execute();
    }

    @Override
    public void delete(DispatchPool pool) {
        deleteById(pool.id());
    }

    @Override
    public boolean deleteById(String id) {
        return dsl.deleteFrom(DISPATCH_POOLS)
            .where(DISPATCH_POOLS.ID.eq(id))
            .execute() > 0;
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private DispatchPool toDomain(Record record) {
        if (record == null) return null;

        return new DispatchPool(
            record.get(DISPATCH_POOLS.ID),
            record.get(DISPATCH_POOLS.CODE),
            record.get(DISPATCH_POOLS.NAME),
            record.get(DISPATCH_POOLS.DESCRIPTION),
            record.get(DISPATCH_POOLS.RATE_LIMIT),
            record.get(DISPATCH_POOLS.CONCURRENCY),
            record.get(DISPATCH_POOLS.CLIENT_ID),
            record.get(DISPATCH_POOLS.CLIENT_IDENTIFIER),
            parseEnum(record.get(DISPATCH_POOLS.STATUS), DispatchPoolStatus.class),
            toInstant(record.get(DISPATCH_POOLS.CREATED_AT)),
            toInstant(record.get(DISPATCH_POOLS.UPDATED_AT))
        );
    }

    private DispatchPoolsRecord toRecord(DispatchPool p) {
        DispatchPoolsRecord rec = new DispatchPoolsRecord();
        rec.setId(p.id());
        rec.setCode(p.code());
        rec.setName(p.name());
        rec.setDescription(p.description());
        rec.setRateLimit(p.rateLimit());
        rec.setConcurrency(p.concurrency());
        rec.setClientId(p.clientId());
        rec.setClientIdentifier(p.clientIdentifier());
        rec.setStatus(p.status() != null ? p.status().name() : DispatchPoolStatus.ACTIVE.name());
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
