package tech.flowcatalyst.platform.application;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Application entity.
 */
@RegisterRowMapper(ApplicationRowMapper.class)
public interface ApplicationDao {

    @SqlQuery("SELECT * FROM applications WHERE id = :id")
    Optional<Application> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM applications WHERE code = :code")
    Optional<Application> findByCode(@Bind("code") String code);

    @SqlQuery("SELECT COUNT(*) > 0 FROM applications WHERE code = :code")
    boolean existsByCode(@Bind("code") String code);

    @SqlQuery("SELECT * FROM applications WHERE active = true ORDER BY name")
    List<Application> findAllActive();

    @SqlQuery("SELECT * FROM applications WHERE type = :type AND (:activeOnly = false OR active = true) ORDER BY name")
    List<Application> findByType(@Bind("type") String type, @Bind("activeOnly") boolean activeOnly);

    @SqlQuery("SELECT * FROM applications ORDER BY name")
    List<Application> listAll();

    @SqlUpdate("""
        INSERT INTO applications (id, code, name, description, type, default_base_url,
            service_account_id, active, created_at, updated_at)
        VALUES (:id, :code, :name, :description, :type, :defaultBaseUrl,
            :serviceAccountId, :active, :createdAt, :updatedAt)
        """)
    void insert(@BindFields Application application, @Bind("type") String type);

    @SqlUpdate("""
        UPDATE applications SET code = :code, name = :name, description = :description,
            type = :type, default_base_url = :defaultBaseUrl, service_account_id = :serviceAccountId,
            active = :active, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields Application application, @Bind("type") String type);

    @SqlUpdate("DELETE FROM applications WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
