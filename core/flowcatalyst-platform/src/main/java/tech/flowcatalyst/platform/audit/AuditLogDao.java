package tech.flowcatalyst.platform.audit;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for AuditLog entity.
 */
@RegisterRowMapper(AuditLogRowMapper.class)
public interface AuditLogDao {

    @SqlQuery("SELECT * FROM audit_logs WHERE id = :id")
    Optional<AuditLog> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM audit_logs WHERE entity_type = :entityType AND entity_id = :entityId ORDER BY performed_at DESC")
    List<AuditLog> findByEntity(@Bind("entityType") String entityType, @Bind("entityId") String entityId);

    @SqlQuery("SELECT * FROM audit_logs WHERE principal_id = :principalId ORDER BY performed_at DESC")
    List<AuditLog> findByPrincipal(@Bind("principalId") String principalId);

    @SqlQuery("SELECT * FROM audit_logs WHERE operation = :operation ORDER BY performed_at DESC")
    List<AuditLog> findByOperation(@Bind("operation") String operation);

    @SqlQuery("SELECT * FROM audit_logs ORDER BY performed_at DESC LIMIT :pageSize OFFSET :offset")
    List<AuditLog> findPaged(@Bind("offset") int offset, @Bind("pageSize") int pageSize);

    @SqlQuery("SELECT * FROM audit_logs WHERE entity_type = :entityType ORDER BY performed_at DESC LIMIT :pageSize OFFSET :offset")
    List<AuditLog> findByEntityTypePaged(@Bind("entityType") String entityType, @Bind("offset") int offset, @Bind("pageSize") int pageSize);

    @SqlQuery("SELECT COUNT(*) FROM audit_logs")
    long count();

    @SqlQuery("SELECT COUNT(*) FROM audit_logs WHERE entity_type = :entityType")
    long countByEntityType(@Bind("entityType") String entityType);

    @SqlQuery("SELECT DISTINCT entity_type FROM audit_logs ORDER BY entity_type")
    List<String> findDistinctEntityTypes();

    @SqlQuery("SELECT DISTINCT operation FROM audit_logs ORDER BY operation")
    List<String> findDistinctOperations();

    @SqlUpdate("""
        INSERT INTO audit_logs (id, entity_type, entity_id, operation, operation_json, principal_id, performed_at)
        VALUES (:id, :entityType, :entityId, :operation, :operationJson::jsonb, :principalId, :performedAt)
        """)
    void insert(@BindBean AuditLog log);
}
