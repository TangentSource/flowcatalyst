package tech.flowcatalyst.platform.common.jooq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import tech.flowcatalyst.event.ContextData;
import tech.flowcatalyst.event.Event;
import tech.flowcatalyst.platform.audit.AuditLog;
import tech.flowcatalyst.platform.common.DomainEvent;
import tech.flowcatalyst.platform.common.Result;
import tech.flowcatalyst.platform.common.UnitOfWork;
import tech.flowcatalyst.platform.common.errors.UseCaseError;
import tech.flowcatalyst.platform.shared.TsidGenerator;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static tech.flowcatalyst.platform.jooq.generated.tables.AuditLogs.AUDIT_LOGS;
import static tech.flowcatalyst.platform.jooq.generated.tables.Events.EVENTS;

/**
 * JOOQ implementation of {@link UnitOfWork} using JOOQ transactions.
 *
 * <p>This implementation ensures atomic commits of:
 * <ul>
 *   <li>Aggregate entity (create/update/delete)</li>
 *   <li>Domain event (in the events table)</li>
 *   <li>Audit log entry</li>
 * </ul>
 *
 * <p>All three operations occur within a single JDBC transaction managed by JOOQ,
 * ensuring consistency. If any operation fails, the entire transaction is rolled back.
 */
@ApplicationScoped
@jakarta.annotation.Priority(2) // Higher priority than PostgresTransactionalUnitOfWork
public class JooqTransactionalUnitOfWork implements UnitOfWork {

    @Inject
    DSLContext dsl;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    JooqAggregateRegistry aggregateRegistry;

    @Override
    public <T extends DomainEvent> Result<T> commit(
            Object aggregate,
            T event,
            Object command
    ) {
        try {
            return dsl.transactionResult(config -> {
                DSLContext ctx = DSL.using(config);
                try {
                    // 1. Persist/update aggregate
                    aggregateRegistry.persist(ctx, aggregate);

                    // 2. Create domain event
                    createEvent(ctx, event);

                    // 3. Create audit log
                    createAuditLog(ctx, event, command);

                    return Result.success(event);

                } catch (Exception e) {
                    throw new RuntimeException("Transaction failed: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            return Result.failure(new UseCaseError.BusinessRuleViolation(
                "COMMIT_FAILED",
                "Failed to commit transaction: " + e.getMessage(),
                Map.of("exception", e.getClass().getSimpleName())
            ));
        }
    }

    @Override
    public <T extends DomainEvent> Result<T> commitDelete(
            Object aggregate,
            T event,
            Object command
    ) {
        try {
            return dsl.transactionResult(config -> {
                DSLContext ctx = DSL.using(config);
                try {
                    // 1. Delete aggregate
                    aggregateRegistry.delete(ctx, aggregate);

                    // 2. Create domain event
                    createEvent(ctx, event);

                    // 3. Create audit log
                    createAuditLog(ctx, event, command);

                    return Result.success(event);

                } catch (Exception e) {
                    throw new RuntimeException("Transaction failed: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            return Result.failure(new UseCaseError.BusinessRuleViolation(
                "COMMIT_FAILED",
                "Failed to commit transaction: " + e.getMessage(),
                Map.of("exception", e.getClass().getSimpleName())
            ));
        }
    }

    @Override
    public <T extends DomainEvent> Result<T> commitAll(
            List<Object> aggregates,
            T event,
            Object command
    ) {
        try {
            return dsl.transactionResult(config -> {
                DSLContext ctx = DSL.using(config);
                try {
                    // 1. Persist/update all aggregates
                    for (Object aggregate : aggregates) {
                        aggregateRegistry.persist(ctx, aggregate);
                    }

                    // 2. Create domain event
                    createEvent(ctx, event);

                    // 3. Create audit log
                    createAuditLog(ctx, event, command);

                    return Result.success(event);

                } catch (Exception e) {
                    throw new RuntimeException("Transaction failed: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            return Result.failure(new UseCaseError.BusinessRuleViolation(
                "COMMIT_FAILED",
                "Failed to commit transaction: " + e.getMessage(),
                Map.of("exception", e.getClass().getSimpleName())
            ));
        }
    }

    // ========================================================================
    // Event Operations
    // ========================================================================

    private void createEvent(DSLContext ctx, DomainEvent event) throws JsonProcessingException {
        // Build context data for searchability
        List<ContextData> contextData = new ArrayList<>();
        contextData.add(new ContextData("principalId", String.valueOf(event.principalId())));
        contextData.add(new ContextData("aggregateType", extractAggregateType(event.subject())));

        String contextDataJson = objectMapper.writeValueAsString(contextData);

        ctx.insertInto(EVENTS)
            .set(EVENTS.ID, event.eventId())
            .set(EVENTS.SPEC_VERSION, event.specVersion())
            .set(EVENTS.TYPE, event.eventType())
            .set(EVENTS.SOURCE, event.source())
            .set(EVENTS.SUBJECT, event.subject())
            .set(EVENTS.TIME, toOffsetDateTime(event.time()))
            .set(EVENTS.DATA, event.toDataJson())
            .set(EVENTS.CORRELATION_ID, event.correlationId())
            .set(EVENTS.CAUSATION_ID, event.causationId())
            .set(EVENTS.DEDUPLICATION_ID, event.eventType() + "-" + event.eventId())
            .set(EVENTS.MESSAGE_GROUP, event.messageGroup())
            .set(EVENTS.CONTEXT_DATA, contextDataJson)
            .set(EVENTS.CREATED_AT, OffsetDateTime.now(ZoneOffset.UTC))
            .execute();
    }

    // ========================================================================
    // Audit Log Operations
    // ========================================================================

    private void createAuditLog(DSLContext ctx, DomainEvent event, Object command)
            throws JsonProcessingException {
        String operationJson = objectMapper.writeValueAsString(command);

        ctx.insertInto(AUDIT_LOGS)
            .set(AUDIT_LOGS.ID, TsidGenerator.generate())
            .set(AUDIT_LOGS.ENTITY_TYPE, extractAggregateType(event.subject()))
            .set(AUDIT_LOGS.ENTITY_ID, extractEntityIdFromSubject(event.subject()))
            .set(AUDIT_LOGS.OPERATION, command.getClass().getSimpleName())
            .set(AUDIT_LOGS.OPERATION_JSON, operationJson)
            .set(AUDIT_LOGS.PRINCIPAL_ID, event.principalId())
            .set(AUDIT_LOGS.PERFORMED_AT, toOffsetDateTime(event.time()))
            .execute();
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Extract the aggregate type from a subject string.
     *
     * <p>Subject format: "platform.eventtype.123456789"
     * <p>Returns: "EventType" (capitalized)
     */
    private String extractAggregateType(String subject) {
        if (subject == null) {
            return "Unknown";
        }
        String[] parts = subject.split("\\.");
        if (parts.length >= 2) {
            return capitalize(parts[1].replace("-", ""));
        }
        return "Unknown";
    }

    /**
     * Extract the entity ID from a subject string.
     *
     * <p>Subject format: "platform.eventtype.123456789"
     * <p>Returns: "123456789"
     */
    private String extractEntityIdFromSubject(String subject) {
        if (subject == null) {
            return null;
        }
        String[] parts = subject.split("\\.");
        if (parts.length >= 3) {
            return parts[2];
        }
        return null;
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }
}
