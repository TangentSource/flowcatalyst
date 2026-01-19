package tech.flowcatalyst.platform.common.panache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import tech.flowcatalyst.event.ContextData;
import tech.flowcatalyst.platform.common.DomainEvent;
import tech.flowcatalyst.platform.common.Result;
import tech.flowcatalyst.platform.common.UnitOfWork;
import tech.flowcatalyst.platform.common.errors.UseCaseError;
import tech.flowcatalyst.platform.shared.TsidGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Panache/JPA implementation of {@link UnitOfWork} using JTA transactions.
 *
 * <p>This implementation ensures atomic commits of:
 * <ul>
 *   <li>Aggregate entity (create/update/delete)</li>
 *   <li>Domain event (in the events table)</li>
 *   <li>Audit log entry</li>
 * </ul>
 *
 * <p>All three operations occur within a single JTA transaction managed by Quarkus,
 * ensuring consistency. If any operation fails, the entire transaction is rolled back.
 */
@ApplicationScoped
public class PanacheTransactionalUnitOfWork implements UnitOfWork {

    @Inject
    EntityManager em;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    PanacheAggregateRegistry aggregateRegistry;

    @Override
    @Transactional
    public <T extends DomainEvent> Result<T> commit(
            Object aggregate,
            T event,
            Object command
    ) {
        try {
            // 1. Persist/update aggregate
            aggregateRegistry.persist(aggregate);

            // 2. Create domain event
            createEvent(event);

            // 3. Create audit log
            createAuditLog(event, command);

            return Result.success(event);

        } catch (Exception e) {
            return Result.failure(new UseCaseError.BusinessRuleViolation(
                "COMMIT_FAILED",
                "Failed to commit transaction: " + e.getMessage(),
                Map.of("exception", e.getClass().getSimpleName())
            ));
        }
    }

    @Override
    @Transactional
    public <T extends DomainEvent> Result<T> commitDelete(
            Object aggregate,
            T event,
            Object command
    ) {
        try {
            // 1. Delete aggregate
            aggregateRegistry.delete(aggregate);

            // 2. Create domain event
            createEvent(event);

            // 3. Create audit log
            createAuditLog(event, command);

            return Result.success(event);

        } catch (Exception e) {
            return Result.failure(new UseCaseError.BusinessRuleViolation(
                "COMMIT_FAILED",
                "Failed to commit transaction: " + e.getMessage(),
                Map.of("exception", e.getClass().getSimpleName())
            ));
        }
    }

    @Override
    @Transactional
    public <T extends DomainEvent> Result<T> commitAll(
            List<Object> aggregates,
            T event,
            Object command
    ) {
        try {
            // 1. Persist/update all aggregates
            for (Object aggregate : aggregates) {
                aggregateRegistry.persist(aggregate);
            }

            // 2. Create domain event
            createEvent(event);

            // 3. Create audit log
            createAuditLog(event, command);

            return Result.success(event);

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

    private void createEvent(DomainEvent event) throws JsonProcessingException {
        // Build context data for searchability
        List<ContextData> contextData = new ArrayList<>();
        contextData.add(new ContextData("principalId", String.valueOf(event.principalId())));
        contextData.add(new ContextData("aggregateType", extractAggregateType(event.subject())));

        String contextDataJson = objectMapper.writeValueAsString(contextData);

        String sql = """
            INSERT INTO events (id, spec_version, type, source, subject, time, data,
                correlation_id, causation_id, deduplication_id, message_group, context_data, created_at)
            VALUES (:id, :specVersion, :type, :source, :subject, :time, :data,
                :correlationId, :causationId, :deduplicationId, :messageGroup, :contextData::jsonb, :createdAt)
            """;

        em.createNativeQuery(sql)
            .setParameter("id", event.eventId())
            .setParameter("specVersion", event.specVersion())
            .setParameter("type", event.eventType())
            .setParameter("source", event.source())
            .setParameter("subject", event.subject())
            .setParameter("time", event.time())
            .setParameter("data", event.toDataJson())
            .setParameter("correlationId", event.correlationId())
            .setParameter("causationId", event.causationId())
            .setParameter("deduplicationId", event.eventType() + "-" + event.eventId())
            .setParameter("messageGroup", event.messageGroup())
            .setParameter("contextData", contextDataJson)
            .setParameter("createdAt", Instant.now())
            .executeUpdate();
    }

    // ========================================================================
    // Audit Log Operations
    // ========================================================================

    private void createAuditLog(DomainEvent event, Object command) throws JsonProcessingException {
        String operationJson = objectMapper.writeValueAsString(command);

        String sql = """
            INSERT INTO audit_logs (id, entity_type, entity_id, operation, operation_json, principal_id, performed_at)
            VALUES (:id, :entityType, :entityId, :operation, :operationJson, :principalId, :performedAt)
            """;

        em.createNativeQuery(sql)
            .setParameter("id", TsidGenerator.generate())
            .setParameter("entityType", extractAggregateType(event.subject()))
            .setParameter("entityId", extractEntityIdFromSubject(event.subject()))
            .setParameter("operation", command.getClass().getSimpleName())
            .setParameter("operationJson", operationJson)
            .setParameter("principalId", event.principalId())
            .setParameter("performedAt", event.time())
            .executeUpdate();
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
