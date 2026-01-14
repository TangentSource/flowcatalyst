package tech.flowcatalyst.platform.cors;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for CorsAllowedOrigin entity.
 */
@RegisterRowMapper(CorsAllowedOriginRowMapper.class)
public interface CorsAllowedOriginDao {

    @SqlQuery("SELECT * FROM cors_allowed_origins WHERE id = :id")
    Optional<CorsAllowedOrigin> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM cors_allowed_origins WHERE origin = :origin")
    Optional<CorsAllowedOrigin> findByOrigin(@Bind("origin") String origin);

    @SqlQuery("SELECT * FROM cors_allowed_origins ORDER BY origin")
    List<CorsAllowedOrigin> listAll();

    @SqlQuery("SELECT COUNT(*) > 0 FROM cors_allowed_origins WHERE origin = :origin")
    boolean existsByOrigin(@Bind("origin") String origin);

    @SqlUpdate("""
        INSERT INTO cors_allowed_origins (id, origin, description, created_by, created_at)
        VALUES (:id, :origin, :description, :createdBy, :createdAt)
        """)
    void insert(@BindFields CorsAllowedOrigin entry);

    @SqlUpdate("DELETE FROM cors_allowed_origins WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
