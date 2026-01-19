package tech.flowcatalyst.platform.principal.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import tech.flowcatalyst.platform.principal.Principal;
import tech.flowcatalyst.platform.principal.PrincipalRepository;
import tech.flowcatalyst.platform.principal.PrincipalType;
import tech.flowcatalyst.platform.principal.entity.PrincipalEntity;
import tech.flowcatalyst.platform.principal.entity.PrincipalRoleEntity;
import tech.flowcatalyst.platform.principal.mapper.PrincipalMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of PrincipalRepository.
 *
 * <p>Handles basic CRUD operations. For complex dynamic queries,
 * use QueryDslPrincipalRepository.
 */
@ApplicationScoped
public class PanachePrincipalRepository
    implements PrincipalRepository, PanacheRepositoryBase<PrincipalEntity, String> {

    @Inject
    EntityManager em;

    @Override
    public Principal findById(String id) {
        return findByIdOptional(id).orElse(null);
    }

    @Override
    public Optional<Principal> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(id);
                return p;
            });
    }

    @Override
    public Optional<Principal> findByEmail(String email) {
        return find("email", email)
            .firstResultOptional()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            });
    }

    @Override
    public Optional<Principal> findByServiceAccountCode(String code) {
        // Use native query for JSONB search
        List<PrincipalEntity> results = em.createNativeQuery(
            "SELECT * FROM principals WHERE service_account->>'code' = :code",
            PrincipalEntity.class)
            .setParameter("code", code)
            .getResultList();

        if (results.isEmpty()) {
            return Optional.empty();
        }
        Principal p = PrincipalMapper.toDomain(results.get(0));
        p.roles = loadRoles(p.id);
        return Optional.of(p);
    }

    @Override
    public List<Principal> findByType(PrincipalType type) {
        return find("type", type)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByClientId(String clientId) {
        return find("clientId", clientId)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return find("id in ?1", ids)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findUsersByClientId(String clientId) {
        return find("clientId = ?1 and type = ?2", clientId, PrincipalType.USER)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findActiveUsersByClientId(String clientId) {
        return find("clientId = ?1 and type = ?2 and active = true", clientId, PrincipalType.USER)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByClientIdAndTypeAndActive(String clientId, PrincipalType type, Boolean active) {
        return find("clientId = ?1 and type = ?2 and active = ?3", clientId, type, active)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByClientIdAndType(String clientId, PrincipalType type) {
        return find("clientId = ?1 and type = ?2", clientId, type)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByClientIdAndActive(String clientId, Boolean active) {
        return find("clientId = ?1 and active = ?2", clientId, active)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> findByActive(Boolean active) {
        return find("active", active)
            .stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public List<Principal> listAll() {
        return findAll().stream()
            .map(entity -> {
                Principal p = PrincipalMapper.toDomain(entity);
                p.roles = loadRoles(entity.id);
                return p;
            })
            .toList();
    }

    @Override
    public Optional<Principal> findByServiceAccountClientId(String clientId) {
        // Use native query for JSONB search
        List<PrincipalEntity> results = em.createNativeQuery(
            "SELECT * FROM principals WHERE type = 'SERVICE' AND service_account->>'clientId' = :clientId",
            PrincipalEntity.class)
            .setParameter("clientId", clientId)
            .getResultList();

        if (results.isEmpty()) {
            return Optional.empty();
        }
        Principal p = PrincipalMapper.toDomain(results.get(0));
        p.roles = loadRoles(p.id);
        return Optional.of(p);
    }

    @Override
    public long countByEmailDomain(String domain) {
        return count("emailDomain", domain);
    }

    @Override
    public void persist(Principal principal) {
        if (principal.createdAt == null) {
            principal.createdAt = Instant.now();
        }
        principal.updatedAt = Instant.now();

        PrincipalEntity entity = PrincipalMapper.toEntity(principal);
        persist(entity);

        // Save roles to normalized table
        saveRoles(principal.id, principal.roles);
    }

    @Override
    public void update(Principal principal) {
        principal.updatedAt = Instant.now();

        PrincipalEntity entity = findById(principal.id);
        if (entity != null) {
            PrincipalMapper.updateEntity(entity, principal);
        }

        // Update roles in normalized table
        saveRoles(principal.id, principal.roles);
    }

    @Override
    public boolean deleteById(String id) {
        // Delete roles first (or use ON DELETE CASCADE)
        em.createQuery("DELETE FROM PrincipalRoleEntity WHERE principalId = :id")
            .setParameter("id", id)
            .executeUpdate();

        return delete("id", id) > 0;
    }

    // ========================================================================
    // Role Management
    // ========================================================================

    /**
     * Load roles from the normalized principal_roles table.
     */
    private List<Principal.RoleAssignment> loadRoles(String principalId) {
        List<PrincipalRoleEntity> roleEntities = em.createQuery(
            "SELECT r FROM PrincipalRoleEntity r WHERE r.principalId = :id",
            PrincipalRoleEntity.class)
            .setParameter("id", principalId)
            .getResultList();

        return PrincipalMapper.toRoleAssignments(roleEntities);
    }

    /**
     * Save roles to the normalized principal_roles table.
     * Replaces all existing roles for the principal.
     */
    private void saveRoles(String principalId, List<Principal.RoleAssignment> roles) {
        // Delete existing roles
        em.createQuery("DELETE FROM PrincipalRoleEntity WHERE principalId = :id")
            .setParameter("id", principalId)
            .executeUpdate();

        // Insert new roles
        if (roles != null) {
            List<PrincipalRoleEntity> entities = PrincipalMapper.toRoleEntities(principalId, roles);
            for (PrincipalRoleEntity entity : entities) {
                em.persist(entity);
            }
        }
    }
}
