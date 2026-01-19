package tech.flowcatalyst.platform.audit.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import tech.flowcatalyst.platform.audit.AuditLog;
import tech.flowcatalyst.platform.audit.AuditLogRepository;
import tech.flowcatalyst.platform.common.Page;
import tech.flowcatalyst.platform.jooq.generated.tables.records.AuditLogsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static tech.flowcatalyst.platform.jooq.generated.tables.AuditLogs.AUDIT_LOGS;

/**
 * JOOQ-based implementation of AuditLogRepository.
 */
@ApplicationScoped
public class JooqAuditLogRepository implements AuditLogRepository {

    @Inject
    DSLContext dsl;

    @Override
    public AuditLog findById(String id) {
        return dsl.selectFrom(AUDIT_LOGS)
            .where(AUDIT_LOGS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public List<AuditLog> findByEntity(String entityType, String entityId) {
        return dsl.selectFrom(AUDIT_LOGS)
            .where(AUDIT_LOGS.ENTITY_TYPE.eq(entityType))
            .and(AUDIT_LOGS.ENTITY_ID.eq(entityId))
            .orderBy(AUDIT_LOGS.PERFORMED_AT.desc())
            .fetch(this::toDomain);
    }

    @Override
    public List<AuditLog> findByPrincipal(String principalId) {
        return dsl.selectFrom(AUDIT_LOGS)
            .where(AUDIT_LOGS.PRINCIPAL_ID.eq(principalId))
            .orderBy(AUDIT_LOGS.PERFORMED_AT.desc())
            .fetch(this::toDomain);
    }

    @Override
    public List<AuditLog> findByOperation(String operation) {
        return dsl.selectFrom(AUDIT_LOGS)
            .where(AUDIT_LOGS.OPERATION.eq(operation))
            .orderBy(AUDIT_LOGS.PERFORMED_AT.desc())
            .fetch(this::toDomain);
    }

    @Override
    public List<AuditLog> findPaged(int page, int pageSize) {
        return dsl.selectFrom(AUDIT_LOGS)
            .orderBy(AUDIT_LOGS.PERFORMED_AT.desc())
            .limit(pageSize)
            .offset(page * pageSize)
            .fetch(this::toDomain);
    }

    @Override
    public List<AuditLog> findByEntityTypePaged(String entityType, int page, int pageSize) {
        return dsl.selectFrom(AUDIT_LOGS)
            .where(AUDIT_LOGS.ENTITY_TYPE.eq(entityType))
            .orderBy(AUDIT_LOGS.PERFORMED_AT.desc())
            .limit(pageSize)
            .offset(page * pageSize)
            .fetch(this::toDomain);
    }

    @Override
    @Deprecated
    public long count() {
        return dsl.selectCount()
            .from(AUDIT_LOGS)
            .fetchOne(0, Long.class);
    }

    @Override
    @Deprecated
    public long countByEntityType(String entityType) {
        return dsl.selectCount()
            .from(AUDIT_LOGS)
            .where(AUDIT_LOGS.ENTITY_TYPE.eq(entityType))
            .fetchOne(0, Long.class);
    }

    @Override
    public Page<AuditLog> findPage(String afterCursor, int limit) {
        // Fetch one extra to detect if there are more pages
        List<AuditLog> logs = dsl.selectFrom(AUDIT_LOGS)
            .where(afterCursor != null ? AUDIT_LOGS.ID.gt(afterCursor) : DSL.noCondition())
            .orderBy(AUDIT_LOGS.ID.asc())
            .limit(limit + 1)
            .fetch(this::toDomain);

        return Page.of(logs, limit, log -> log.id);
    }

    @Override
    public List<String> findDistinctEntityTypes() {
        return dsl.selectDistinct(AUDIT_LOGS.ENTITY_TYPE)
            .from(AUDIT_LOGS)
            .orderBy(AUDIT_LOGS.ENTITY_TYPE)
            .fetch(AUDIT_LOGS.ENTITY_TYPE);
    }

    @Override
    public List<String> findDistinctOperations() {
        return dsl.selectDistinct(AUDIT_LOGS.OPERATION)
            .from(AUDIT_LOGS)
            .orderBy(AUDIT_LOGS.OPERATION)
            .fetch(AUDIT_LOGS.OPERATION);
    }

    @Override
    public void persist(AuditLog log) {
        AuditLogsRecord record = toRecord(log);
        record.setPerformedAt(toOffsetDateTime(log.performedAt));
        dsl.insertInto(AUDIT_LOGS).set(record).execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private AuditLog toDomain(Record record) {
        if (record == null) return null;

        AuditLog log = new AuditLog();
        log.id = record.get(AUDIT_LOGS.ID);
        log.entityType = record.get(AUDIT_LOGS.ENTITY_TYPE);
        log.entityId = record.get(AUDIT_LOGS.ENTITY_ID);
        log.operation = record.get(AUDIT_LOGS.OPERATION);
        log.operationJson = record.get(AUDIT_LOGS.OPERATION_JSON);
        log.principalId = record.get(AUDIT_LOGS.PRINCIPAL_ID);
        log.performedAt = toInstant(record.get(AUDIT_LOGS.PERFORMED_AT));

        return log;
    }

    private AuditLogsRecord toRecord(AuditLog log) {
        AuditLogsRecord rec = new AuditLogsRecord();
        rec.setId(log.id);
        rec.setEntityType(log.entityType);
        rec.setEntityId(log.entityId);
        rec.setOperation(log.operation);
        rec.setOperationJson(log.operationJson);
        rec.setPrincipalId(log.principalId);
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
