package tech.flowcatalyst.platform.authorization.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.authorization.AuthPermission;
import tech.flowcatalyst.platform.authorization.AuthPermissionRepository;
import tech.flowcatalyst.platform.authorization.entity.AuthPermissionEntity;
import tech.flowcatalyst.platform.authorization.mapper.AuthPermissionMapper;

import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of AuthPermissionRepository.
 */
@ApplicationScoped
public class PanacheAuthPermissionRepository
    implements AuthPermissionRepository, PanacheRepositoryBase<AuthPermissionEntity, String> {

    @Override
    public Optional<AuthPermission> findByName(String name) {
        return find("name", name)
            .firstResultOptional()
            .map(AuthPermissionMapper::toDomain);
    }

    @Override
    public List<AuthPermission> findByApplicationId(String applicationId) {
        return find("applicationId", applicationId)
            .stream()
            .map(AuthPermissionMapper::toDomain)
            .toList();
    }

    @Override
    public List<AuthPermission> listAll() {
        return findAll().stream()
            .map(AuthPermissionMapper::toDomain)
            .toList();
    }

    @Override
    public boolean existsByName(String name) {
        return count("name", name) > 0;
    }

    @Override
    public void persist(AuthPermission permission) {
        AuthPermissionEntity entity = AuthPermissionMapper.toEntity(permission);
        persist(entity);
    }

    @Override
    public void update(AuthPermission permission) {
        AuthPermissionEntity entity = findById(permission.id);
        if (entity != null) {
            AuthPermissionMapper.updateEntity(entity, permission);
        }
    }

    @Override
    public void delete(AuthPermission permission) {
        deleteById(permission.id);
    }

    @Override
    public long deleteByApplicationId(String applicationId) {
        return delete("applicationId", applicationId);
    }
}
