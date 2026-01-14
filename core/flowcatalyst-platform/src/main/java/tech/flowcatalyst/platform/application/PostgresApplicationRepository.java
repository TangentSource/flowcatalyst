package tech.flowcatalyst.platform.application;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of ApplicationRepository using JDBI.
 */
@ApplicationScoped
@Typed(ApplicationRepository.class)
class PostgresApplicationRepository implements ApplicationRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Optional<Application> findByIdOptional(String id) {
        return jdbi.withExtension(ApplicationDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Application> findByCode(String code) {
        return jdbi.withExtension(ApplicationDao.class, dao -> dao.findByCode(code));
    }

    @Override
    public List<Application> findAllActive() {
        return jdbi.withExtension(ApplicationDao.class, ApplicationDao::findAllActive);
    }

    @Override
    public List<Application> findByType(Application.ApplicationType type, boolean activeOnly) {
        return jdbi.withExtension(ApplicationDao.class, dao ->
            dao.findByType(type.name(), activeOnly));
    }

    @Override
    public List<Application> findByCodes(Collection<String> codes) {
        if (codes == null || codes.isEmpty()) {
            return new ArrayList<>();
        }
        return jdbi.withHandle(handle ->
            handle.createQuery("SELECT * FROM applications WHERE code = ANY(:codes)")
                .bindArray("codes", String.class, codes.toArray(new String[0]))
                .registerRowMapper(new ApplicationRowMapper())
                .mapTo(Application.class)
                .list()
        );
    }

    @Override
    public List<Application> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return jdbi.withHandle(handle ->
            handle.createQuery("SELECT * FROM applications WHERE id = ANY(:ids)")
                .bindArray("ids", String.class, ids.toArray(new String[0]))
                .registerRowMapper(new ApplicationRowMapper())
                .mapTo(Application.class)
                .list()
        );
    }

    @Override
    public List<Application> listAll() {
        return jdbi.withExtension(ApplicationDao.class, ApplicationDao::listAll);
    }

    @Override
    public boolean existsByCode(String code) {
        return jdbi.withExtension(ApplicationDao.class, dao -> dao.existsByCode(code));
    }

    @Override
    public void persist(Application application) {
        String type = application.type != null ? application.type.name() : "APPLICATION";
        jdbi.useExtension(ApplicationDao.class, dao -> dao.insert(application, type));
    }

    @Override
    public void update(Application application) {
        application.updatedAt = Instant.now();
        String type = application.type != null ? application.type.name() : "APPLICATION";
        jdbi.useExtension(ApplicationDao.class, dao -> dao.update(application, type));
    }

    @Override
    public void delete(Application application) {
        jdbi.useExtension(ApplicationDao.class, dao -> dao.deleteById(application.id));
    }
}
