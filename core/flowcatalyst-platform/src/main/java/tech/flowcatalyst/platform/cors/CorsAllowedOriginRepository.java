package tech.flowcatalyst.platform.cors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of CORS allowed origins repository using JDBI.
 */
@ApplicationScoped
public class CorsAllowedOriginRepository {

    @Inject
    Jdbi jdbi;

    public Optional<CorsAllowedOrigin> findById(String id) {
        return jdbi.withExtension(CorsAllowedOriginDao.class, dao -> dao.findById(id));
    }

    public Optional<CorsAllowedOrigin> findByOrigin(String origin) {
        return jdbi.withExtension(CorsAllowedOriginDao.class, dao -> dao.findByOrigin(origin));
    }

    public List<CorsAllowedOrigin> listAll() {
        return jdbi.withExtension(CorsAllowedOriginDao.class, CorsAllowedOriginDao::listAll);
    }

    public boolean existsByOrigin(String origin) {
        return jdbi.withExtension(CorsAllowedOriginDao.class, dao -> dao.existsByOrigin(origin));
    }

    public void persist(CorsAllowedOrigin entry) {
        jdbi.useExtension(CorsAllowedOriginDao.class, dao -> dao.insert(entry));
    }

    public void delete(CorsAllowedOrigin entry) {
        jdbi.useExtension(CorsAllowedOriginDao.class, dao -> dao.deleteById(entry.id));
    }
}
