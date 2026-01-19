package tech.flowcatalyst.platform.cors.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.cors.CorsAllowedOrigin;
import tech.flowcatalyst.platform.cors.entity.CorsAllowedOriginEntity;
import tech.flowcatalyst.platform.cors.mapper.CorsAllowedOriginMapper;

import java.util.List;
import java.util.Optional;

/**
 * Panache-based implementation of CORS allowed origins repository.
 */
@ApplicationScoped
public class PanacheCorsAllowedOriginRepository
    implements PanacheRepositoryBase<CorsAllowedOriginEntity, String> {

    public Optional<CorsAllowedOrigin> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(CorsAllowedOriginMapper::toDomain);
    }

    public Optional<CorsAllowedOrigin> findByOrigin(String origin) {
        return find("origin", origin)
            .firstResultOptional()
            .map(CorsAllowedOriginMapper::toDomain);
    }

    public List<CorsAllowedOrigin> listAllOrigins() {
        return find("ORDER BY origin")
            .stream()
            .map(CorsAllowedOriginMapper::toDomain)
            .toList();
    }

    public boolean existsByOrigin(String origin) {
        return count("origin", origin) > 0;
    }

    public void persist(CorsAllowedOrigin entry) {
        CorsAllowedOriginEntity entity = CorsAllowedOriginMapper.toEntity(entry);
        persist(entity);
    }

    public void delete(CorsAllowedOrigin entry) {
        deleteById(entry.id);
    }

    public boolean deleteByIdReturning(String id) {
        return deleteById(id);
    }
}
