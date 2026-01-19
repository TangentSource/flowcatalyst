package tech.flowcatalyst.platform.audit.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * JPA entity for audit_logs table.
 */
@Entity
@Table(name = "audit_logs")
public class AuditLogEntity {

    @Id
    @Column(name = "id", length = 17)
    public String id;

    @Column(name = "entity_type", nullable = false)
    public String entityType;

    @Column(name = "entity_id", length = 17)
    public String entityId;

    @Column(name = "operation", nullable = false)
    public String operation;

    @Column(name = "operation_json", columnDefinition = "TEXT")
    public String operationJson;

    @Column(name = "principal_id", length = 17)
    public String principalId;

    @Column(name = "performed_at", nullable = false)
    public Instant performedAt;

    public AuditLogEntity() {
    }
}
