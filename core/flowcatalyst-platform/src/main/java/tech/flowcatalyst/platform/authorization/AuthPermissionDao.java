package tech.flowcatalyst.platform.authorization;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for AuthPermission entity.
 */
@RegisterRowMapper(AuthPermissionRowMapper.class)
public interface AuthPermissionDao {

    @SqlQuery("SELECT * FROM auth_permissions WHERE id = :id")
    Optional<AuthPermission> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM auth_permissions WHERE name = :name")
    Optional<AuthPermission> findByName(@Bind("name") String name);

    @SqlQuery("SELECT * FROM auth_permissions WHERE application_id = :applicationId ORDER BY name")
    List<AuthPermission> findByApplicationId(@Bind("applicationId") String applicationId);

    @SqlQuery("SELECT * FROM auth_permissions ORDER BY name")
    List<AuthPermission> listAll();

    @SqlQuery("SELECT COUNT(*) > 0 FROM auth_permissions WHERE name = :name")
    boolean existsByName(@Bind("name") String name);

    @SqlUpdate("""
        INSERT INTO auth_permissions (id, application_id, name, display_name, description, source, created_at)
        VALUES (:id, :applicationId, :name, :displayName, :description, :source, :createdAt)
        """)
    void insert(@BindFields AuthPermission permission, @Bind("source") String source);

    @SqlUpdate("""
        UPDATE auth_permissions SET application_id = :applicationId, name = :name,
            display_name = :displayName, description = :description, source = :source
        WHERE id = :id
        """)
    void update(@BindFields AuthPermission permission, @Bind("source") String source);

    @SqlUpdate("DELETE FROM auth_permissions WHERE id = :id")
    int deleteById(@Bind("id") String id);

    @SqlUpdate("DELETE FROM auth_permissions WHERE application_id = :applicationId")
    long deleteByApplicationId(@Bind("applicationId") String applicationId);
}
