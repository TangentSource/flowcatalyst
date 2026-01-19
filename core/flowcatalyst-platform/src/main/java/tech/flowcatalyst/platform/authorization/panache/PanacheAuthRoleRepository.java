package tech.flowcatalyst.platform.authorization.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.authorization.AuthRole;
import tech.flowcatalyst.platform.authorization.AuthRoleRepository;
import tech.flowcatalyst.platform.authorization.entity.AuthRoleEntity;
import tech.flowcatalyst.platform.authorization.mapper.AuthRoleMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of AuthRoleRepository.
 */
@ApplicationScoped
public class PanacheAuthRoleRepository
    implements AuthRoleRepository, PanacheRepositoryBase<AuthRoleEntity, String> {

    @Override
    public Optional<AuthRole> findByName(String name) {
        return find("name", name)
            .firstResultOptional()
            .map(AuthRoleMapper::toDomain);
    }

    @Override
    public List<AuthRole> findByApplicationCode(String applicationCode) {
        return find("applicationCode", applicationCode)
            .stream()
            .map(AuthRoleMapper::toDomain)
            .toList();
    }

    @Override
    public List<AuthRole> findBySource(AuthRole.RoleSource source) {
        return find("source", source)
            .stream()
            .map(AuthRoleMapper::toDomain)
            .toList();
    }

    @Override
    public List<AuthRole> listAll() {
        return findAll().stream()
            .map(AuthRoleMapper::toDomain)
            .toList();
    }

    @Override
    public boolean existsByName(String name) {
        return count("name", name) > 0;
    }

    @Override
    public void persist(AuthRole role) {
        if (role.createdAt == null) {
            role.createdAt = Instant.now();
        }
        role.updatedAt = Instant.now();
        AuthRoleEntity entity = AuthRoleMapper.toEntity(role);
        persist(entity);
    }

    @Override
    public void update(AuthRole role) {
        role.updatedAt = Instant.now();
        AuthRoleEntity entity = findById(role.id);
        if (entity != null) {
            AuthRoleMapper.updateEntity(entity, role);
        }
    }

    @Override
    public void delete(AuthRole role) {
        deleteById(role.id);
    }
}
