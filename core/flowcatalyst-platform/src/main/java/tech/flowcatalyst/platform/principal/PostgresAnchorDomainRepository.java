package tech.flowcatalyst.platform.principal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of AnchorDomainRepository using JDBI.
 */
@ApplicationScoped
@Typed(AnchorDomainRepository.class)
class PostgresAnchorDomainRepository implements AnchorDomainRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<AnchorDomain> findByIdOptional(String id) {
        return jdbi.withExtension(AnchorDomainDao.class, dao -> dao.findById(id));
    }

    @Override
    public List<AnchorDomain> listAll() {
        return jdbi.withExtension(AnchorDomainDao.class, AnchorDomainDao::listAll);
    }

    @Override
    public boolean existsByDomain(String domain) {
        return jdbi.withExtension(AnchorDomainDao.class, dao -> dao.existsByDomain(domain));
    }

    @Override
    public void persist(AnchorDomain domain) {
        jdbi.useExtension(AnchorDomainDao.class, dao -> dao.insert(domain));
    }

    @Override
    public void delete(AnchorDomain domain) {
        jdbi.useExtension(AnchorDomainDao.class, dao -> dao.deleteById(domain.id));
    }
}
