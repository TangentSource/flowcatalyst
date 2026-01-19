package tech.flowcatalyst.platform.principal.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.principal.AnchorDomain;
import tech.flowcatalyst.platform.principal.AnchorDomainRepository;
import tech.flowcatalyst.platform.principal.entity.AnchorDomainEntity;
import tech.flowcatalyst.platform.principal.mapper.AnchorDomainMapper;

import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of AnchorDomainRepository.
 *
 * <p>Uses Hibernate ORM Panache for simple CRUD operations with type-safe queries.
 */
@ApplicationScoped
public class PanacheAnchorDomainRepository
    implements AnchorDomainRepository, PanacheRepositoryBase<AnchorDomainEntity, String> {

    @Override
    public Optional<AnchorDomain> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(AnchorDomainMapper::toDomain);
    }

    @Override
    public List<AnchorDomain> listAll() {
        return findAll().stream()
            .map(AnchorDomainMapper::toDomain)
            .toList();
    }

    @Override
    public boolean existsByDomain(String domain) {
        return count("domain", domain) > 0;
    }

    @Override
    public void persist(AnchorDomain domain) {
        AnchorDomainEntity entity = AnchorDomainMapper.toEntity(domain);
        persist(entity);
    }

    @Override
    public void delete(AnchorDomain domain) {
        deleteById(domain.id);
    }

    /**
     * Find anchor domain by domain name.
     */
    public Optional<AnchorDomain> findByDomain(String domain) {
        return find("domain", domain)
            .firstResultOptional()
            .map(AnchorDomainMapper::toDomain);
    }
}
