package tech.flowcatalyst.platform.application.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.application.Application;
import tech.flowcatalyst.platform.application.ApplicationRepository;
import tech.flowcatalyst.platform.application.entity.ApplicationEntity;
import tech.flowcatalyst.platform.application.mapper.ApplicationMapper;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of ApplicationRepository.
 */
@ApplicationScoped
public class PanacheApplicationRepository
    implements ApplicationRepository, PanacheRepositoryBase<ApplicationEntity, String> {

    @Override
    public Optional<Application> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(ApplicationMapper::toDomain);
    }

    @Override
    public Optional<Application> findByCode(String code) {
        return find("code", code)
            .firstResultOptional()
            .map(ApplicationMapper::toDomain);
    }

    @Override
    public List<Application> findAllActive() {
        return find("active", true)
            .stream()
            .map(ApplicationMapper::toDomain)
            .toList();
    }

    @Override
    public List<Application> findByType(Application.ApplicationType type, boolean activeOnly) {
        if (activeOnly) {
            return find("type = ?1 and active = true", type)
                .stream()
                .map(ApplicationMapper::toDomain)
                .toList();
        }
        return find("type", type)
            .stream()
            .map(ApplicationMapper::toDomain)
            .toList();
    }

    @Override
    public List<Application> findByCodes(Collection<String> codes) {
        if (codes == null || codes.isEmpty()) {
            return List.of();
        }
        return find("code in ?1", codes)
            .stream()
            .map(ApplicationMapper::toDomain)
            .toList();
    }

    @Override
    public List<Application> findByIds(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        return find("id in ?1", ids)
            .stream()
            .map(ApplicationMapper::toDomain)
            .toList();
    }

    @Override
    public List<Application> listAll() {
        return findAll().stream()
            .map(ApplicationMapper::toDomain)
            .toList();
    }

    @Override
    public boolean existsByCode(String code) {
        return count("code", code) > 0;
    }

    @Override
    public void persist(Application application) {
        if (application.createdAt == null) {
            application.createdAt = Instant.now();
        }
        application.updatedAt = Instant.now();
        ApplicationEntity entity = ApplicationMapper.toEntity(application);
        persist(entity);
    }

    @Override
    public void update(Application application) {
        application.updatedAt = Instant.now();
        ApplicationEntity entity = findById(application.id);
        if (entity != null) {
            ApplicationMapper.updateEntity(entity, application);
        }
    }

    @Override
    public void delete(Application application) {
        deleteById(application.id);
    }
}
