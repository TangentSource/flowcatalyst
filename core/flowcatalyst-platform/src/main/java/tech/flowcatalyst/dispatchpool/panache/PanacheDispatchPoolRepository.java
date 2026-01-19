package tech.flowcatalyst.dispatchpool.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.dispatchpool.DispatchPool;
import tech.flowcatalyst.dispatchpool.DispatchPoolRepository;
import tech.flowcatalyst.dispatchpool.DispatchPoolStatus;
import tech.flowcatalyst.dispatchpool.entity.DispatchPoolEntity;
import tech.flowcatalyst.dispatchpool.mapper.DispatchPoolMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of DispatchPoolRepository.
 */
@ApplicationScoped
public class PanacheDispatchPoolRepository
    implements DispatchPoolRepository, PanacheRepositoryBase<DispatchPoolEntity, String> {

    @Override
    public DispatchPool findById(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(DispatchPoolMapper::toDomain)
            .orElse(null);
    }

    @Override
    public Optional<DispatchPool> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(DispatchPoolMapper::toDomain);
    }

    @Override
    public Optional<DispatchPool> findByCodeAndClientId(String code, String clientId) {
        if (clientId == null) {
            return find("code = ?1 and clientId is null", code)
                .firstResultOptional()
                .map(DispatchPoolMapper::toDomain);
        }
        return find("code = ?1 and clientId = ?2", code, clientId)
            .firstResultOptional()
            .map(DispatchPoolMapper::toDomain);
    }

    @Override
    public List<DispatchPool> findByClientId(String clientId) {
        return find("clientId", clientId)
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> findAnchorLevel() {
        return find("clientId is null")
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> findByStatus(DispatchPoolStatus status) {
        return find("status", status)
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> findActive() {
        return find("status", DispatchPoolStatus.ACTIVE)
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> findAllNonArchived() {
        return find("status != ?1", DispatchPoolStatus.ARCHIVED)
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> findWithFilters(String clientId, DispatchPoolStatus status, boolean includeArchived) {
        StringBuilder query = new StringBuilder("1=1");

        if (clientId != null) {
            query.append(" and clientId = '").append(clientId).append("'");
        }

        if (status != null) {
            query.append(" and status = '").append(status.name()).append("'");
        }

        if (!includeArchived && status == null) {
            query.append(" and status != '").append(DispatchPoolStatus.ARCHIVED.name()).append("'");
        }

        return find(query.toString())
            .stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public List<DispatchPool> listAll() {
        return findAll().stream()
            .map(DispatchPoolMapper::toDomain)
            .toList();
    }

    @Override
    public boolean existsByCodeAndClientId(String code, String clientId) {
        if (clientId == null) {
            return count("code = ?1 and clientId is null", code) > 0;
        }
        return count("code = ?1 and clientId = ?2", code, clientId) > 0;
    }

    @Override
    public void persist(DispatchPool pool) {
        var updated = pool.toBuilder()
            .createdAt(pool.createdAt() != null ? pool.createdAt() : Instant.now())
            .updatedAt(Instant.now())
            .build();
        DispatchPoolEntity entity = DispatchPoolMapper.toEntity(updated);
        persist(entity);
    }

    @Override
    public void update(DispatchPool pool) {
        var updated = pool.toBuilder()
            .updatedAt(Instant.now())
            .build();
        DispatchPoolEntity entity = findById(pool.id());
        if (entity != null) {
            DispatchPoolMapper.updateEntity(entity, updated);
        }
    }

    @Override
    public void delete(DispatchPool pool) {
        deleteById(pool.id());
    }

    @Override
    public boolean deleteById(String id) {
        return delete("id", id) > 0;
    }
}
