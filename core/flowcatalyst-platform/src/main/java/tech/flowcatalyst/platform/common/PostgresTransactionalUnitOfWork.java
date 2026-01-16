package tech.flowcatalyst.platform.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.event.ContextData;
import tech.flowcatalyst.event.Event;
import tech.flowcatalyst.event.EventDao;
import tech.flowcatalyst.platform.audit.AuditLog;
import tech.flowcatalyst.platform.audit.AuditLogDao;
import tech.flowcatalyst.platform.common.errors.UseCaseError;
import tech.flowcatalyst.platform.shared.TsidGenerator;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL implementation of {@link UnitOfWork} using JDBI transactions.
 *
 * <p>This implementation ensures atomic commits of:
 * <ul>
 *   <li>Aggregate entity (create/update/delete)</li>
 *   <li>Domain event (in the events table)</li>
 *   <li>Audit log entry</li>
 * </ul>
 *
 * <p>All three operations occur within a single JDBC transaction, ensuring
 * consistency. If any operation fails, the entire transaction is rolled back.
 */
@ApplicationScoped
@Alternative
@jakarta.annotation.Priority(1)
public class PostgresTransactionalUnitOfWork implements UnitOfWork {

    @Inject
    Jdbi jdbi;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    AggregateRegistry aggregateRegistry;

    @Override
    public <T extends DomainEvent> Result<T> commit(
            Object aggregate,
            T event,
            Object command
    ) {
        try {
            return jdbi.inTransaction(handle -> {
                try {
                    // 1. Persist/update aggregate
                    persistAggregate(handle, aggregate);

                    // 2. Create domain event
                    createEvent(handle, event);

                    // 3. Create audit log
                    createAuditLog(handle, event, command);

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
            return jdbi.inTransaction(handle -> {
                try {
                    // 1. Delete aggregate
                    deleteAggregate(handle, aggregate);

                    // 2. Create domain event
                    createEvent(handle, event);

                    // 3. Create audit log
                    createAuditLog(handle, event, command);

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
            return jdbi.inTransaction(handle -> {
                try {
                    // 1. Persist/update all aggregates
                    for (Object aggregate : aggregates) {
                        persistAggregate(handle, aggregate);
                    }

                    // 2. Create domain event
                    createEvent(handle, event);

                    // 3. Create audit log
                    createAuditLog(handle, event, command);

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
    // Aggregate Operations
    // ========================================================================

    private void persistAggregate(Handle handle, Object aggregate) {
        aggregateRegistry.persist(handle, aggregate);
    }

    private void deleteAggregate(Handle handle, Object aggregate) {
        aggregateRegistry.delete(handle, aggregate);
    }

    // ========================================================================
    // Event Operations
    // ========================================================================

    private void createEvent(Handle handle, DomainEvent event) throws JsonProcessingException {
        // Build context data for searchability
        List<ContextData> contextData = new ArrayList<>();
        contextData.add(new ContextData("principalId", String.valueOf(event.principalId())));
        contextData.add(new ContextData("aggregateType", extractAggregateType(event.subject())));

        Event dbEvent = new Event(
            event.eventId(),
            event.specVersion(),
            event.eventType(),
            event.source(),
            event.subject(),
            event.time(),
            event.toDataJson(),
            event.correlationId(),
            event.causationId(),
            event.eventType() + "-" + event.eventId(),  // deduplicationId
            event.messageGroup(),
            contextData
        );

        String contextDataJson = objectMapper.writeValueAsString(contextData);
        handle.attach(EventDao.class).insert(dbEvent, contextDataJson);
    }

    // ========================================================================
    // Audit Log Operations
    // ========================================================================

    private void createAuditLog(Handle handle, DomainEvent event, Object command)
            throws JsonProcessingException {
        AuditLog log = new AuditLog();
        log.id = TsidGenerator.generate();
        log.entityType = extractAggregateType(event.subject());
        log.entityId = extractEntityIdFromSubject(event.subject());
        log.operation = command.getClass().getSimpleName();
        log.operationJson = objectMapper.writeValueAsString(command);
        log.principalId = event.principalId();
        log.performedAt = event.time();

        handle.attach(AuditLogDao.class).insert(log);
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
}
