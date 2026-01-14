package tech.flowcatalyst.platform.application;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for ApplicationClientConfig entity.
 */
@RegisterRowMapper(ApplicationClientConfigRowMapper.class)
public interface ApplicationClientConfigDao {

    @SqlQuery("SELECT * FROM application_client_configs WHERE id = :id")
    Optional<ApplicationClientConfig> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM application_client_configs WHERE application_id = :applicationId AND client_id = :clientId")
    Optional<ApplicationClientConfig> findByApplicationAndClient(@Bind("applicationId") String applicationId,
                                                                  @Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM application_client_configs WHERE application_id = :applicationId ORDER BY client_id")
    List<ApplicationClientConfig> findByApplication(@Bind("applicationId") String applicationId);

    @SqlQuery("SELECT * FROM application_client_configs WHERE client_id = :clientId ORDER BY application_id")
    List<ApplicationClientConfig> findByClient(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM application_client_configs WHERE client_id = :clientId AND enabled = true ORDER BY application_id")
    List<ApplicationClientConfig> findEnabledByClient(@Bind("clientId") String clientId);

    @SqlQuery("""
        SELECT COALESCE(
            (SELECT enabled FROM application_client_configs WHERE application_id = :applicationId AND client_id = :clientId),
            true
        )
        """)
    boolean isApplicationEnabledForClient(@Bind("applicationId") String applicationId, @Bind("clientId") String clientId);

    @SqlQuery("SELECT COUNT(*) FROM application_client_configs WHERE application_id = :applicationId")
    long countByApplication(@Bind("applicationId") String applicationId);

    @SqlUpdate("""
        INSERT INTO application_client_configs (id, application_id, client_id, enabled, base_url_override,
            config_json, created_at, updated_at)
        VALUES (:id, :applicationId, :clientId, :enabled, :baseUrlOverride,
            :configJson::jsonb, :createdAt, :updatedAt)
        """)
    void insert(@BindBean ApplicationClientConfig config, @Bind("configJson") String configJson);

    @SqlUpdate("""
        UPDATE application_client_configs SET application_id = :applicationId, client_id = :clientId,
            enabled = :enabled, base_url_override = :baseUrlOverride, config_json = :configJson::jsonb,
            updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean ApplicationClientConfig config, @Bind("configJson") String configJson);

    @SqlUpdate("DELETE FROM application_client_configs WHERE id = :id")
    int deleteById(@Bind("id") String id);

    @SqlUpdate("DELETE FROM application_client_configs WHERE application_id = :applicationId AND client_id = :clientId")
    int deleteByApplicationAndClient(@Bind("applicationId") String applicationId, @Bind("clientId") String clientId);
}
