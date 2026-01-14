package tech.flowcatalyst.platform.audit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.List;

/**
 * PostgreSQL implementation of AuditLogRepository using JDBI.
 */
@ApplicationScoped
@Typed(AuditLogRepository.class)
class PostgresAuditLogRepository implements AuditLogRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public AuditLog findById(String id) {
        return jdbi.withExtension(AuditLogDao.class, dao ->
            dao.findById(id).orElse(null));
    }

    @Override
    public List<AuditLog> findByEntity(String entityType, String entityId) {
        return jdbi.withExtension(AuditLogDao.class, dao ->
            dao.findByEntity(entityType, entityId));
    }

    @Override
    public List<AuditLog> findByPrincipal(String principalId) {
        return jdbi.withExtension(AuditLogDao.class, dao -> dao.findByPrincipal(principalId));
    }

    @Override
    public List<AuditLog> findByOperation(String operation) {
        return jdbi.withExtension(AuditLogDao.class, dao -> dao.findByOperation(operation));
    }

    @Override
    public List<AuditLog> findPaged(int page, int pageSize) {
        int offset = page * pageSize;
        return jdbi.withExtension(AuditLogDao.class, dao -> dao.findPaged(offset, pageSize));
    }

    @Override
    public List<AuditLog> findByEntityTypePaged(String entityType, int page, int pageSize) {
        int offset = page * pageSize;
        return jdbi.withExtension(AuditLogDao.class, dao ->
            dao.findByEntityTypePaged(entityType, offset, pageSize));
    }

    @Override
    public long count() {
        return jdbi.withExtension(AuditLogDao.class, AuditLogDao::count);
    }

    @Override
    public long countByEntityType(String entityType) {
        return jdbi.withExtension(AuditLogDao.class, dao -> dao.countByEntityType(entityType));
    }

    @Override
    public List<String> findDistinctEntityTypes() {
        return jdbi.withExtension(AuditLogDao.class, AuditLogDao::findDistinctEntityTypes);
    }

    @Override
    public List<String> findDistinctOperations() {
        return jdbi.withExtension(AuditLogDao.class, AuditLogDao::findDistinctOperations);
    }

    @Override
    public void persist(AuditLog log) {
        jdbi.useExtension(AuditLogDao.class, dao -> dao.insert(log));
    }
}
