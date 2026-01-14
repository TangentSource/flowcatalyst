package tech.flowcatalyst.platform.authorization;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for AuthRole entity.
 */
@RegisterRowMapper(AuthRoleRowMapper.class)
public interface AuthRoleDao {

    @SqlQuery("SELECT * FROM auth_roles WHERE name = :name")
    Optional<AuthRole> findByName(@Bind("name") String name);

    @SqlQuery("SELECT * FROM auth_roles WHERE application_code = :applicationCode ORDER BY name")
    List<AuthRole> findByApplicationCode(@Bind("applicationCode") String applicationCode);

    @SqlQuery("SELECT * FROM auth_roles WHERE source = :source ORDER BY name")
    List<AuthRole> findBySource(@Bind("source") String source);

    @SqlQuery("SELECT * FROM auth_roles ORDER BY name")
    List<AuthRole> listAll();

    @SqlQuery("SELECT COUNT(*) > 0 FROM auth_roles WHERE name = :name")
    boolean existsByName(@Bind("name") String name);

    @SqlUpdate("""
        INSERT INTO auth_roles (id, application_id, application_code, name, display_name,
            description, permissions, source, client_managed, created_at, updated_at)
        VALUES (:id, :applicationId, :applicationCode, :name, :displayName,
            :description, :permissions, :source, :clientManaged, :createdAt, :updatedAt)
        """)
    void insert(@BindFields AuthRole role, @Bind("permissions") String[] permissionsArray, @Bind("source") String source);

    @SqlUpdate("""
        UPDATE auth_roles SET application_id = :applicationId, application_code = :applicationCode,
            name = :name, display_name = :displayName, description = :description,
            permissions = :permissions, source = :source, client_managed = :clientManaged,
            updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields AuthRole role, @Bind("permissions") String[] permissionsArray, @Bind("source") String source);

    @SqlUpdate("DELETE FROM auth_roles WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
