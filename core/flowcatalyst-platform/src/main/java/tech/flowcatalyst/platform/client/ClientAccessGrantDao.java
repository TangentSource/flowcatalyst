package tech.flowcatalyst.platform.client;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for ClientAccessGrant entity.
 */
@RegisterRowMapper(ClientAccessGrantRowMapper.class)
public interface ClientAccessGrantDao {

    @SqlQuery("SELECT * FROM client_access_grants WHERE principal_id = :principalId ORDER BY granted_at DESC")
    List<ClientAccessGrant> findByPrincipalId(@Bind("principalId") String principalId);

    @SqlQuery("SELECT * FROM client_access_grants WHERE client_id = :clientId ORDER BY granted_at DESC")
    List<ClientAccessGrant> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM client_access_grants WHERE principal_id = :principalId AND client_id = :clientId")
    Optional<ClientAccessGrant> findByPrincipalIdAndClientId(@Bind("principalId") String principalId,
                                                              @Bind("clientId") String clientId);

    @SqlQuery("SELECT COUNT(*) > 0 FROM client_access_grants WHERE principal_id = :principalId AND client_id = :clientId")
    boolean existsByPrincipalIdAndClientId(@Bind("principalId") String principalId, @Bind("clientId") String clientId);

    @SqlUpdate("""
        INSERT INTO client_access_grants (id, principal_id, client_id, granted_at, expires_at)
        VALUES (:id, :principalId, :clientId, :grantedAt, :expiresAt)
        """)
    void insert(@BindBean ClientAccessGrant grant);

    @SqlUpdate("DELETE FROM client_access_grants WHERE id = :id")
    int deleteById(@Bind("id") String id);

    @SqlUpdate("DELETE FROM client_access_grants WHERE principal_id = :principalId")
    int deleteByPrincipalId(@Bind("principalId") String principalId);

    @SqlUpdate("DELETE FROM client_access_grants WHERE principal_id = :principalId AND client_id = :clientId")
    int deleteByPrincipalIdAndClientId(@Bind("principalId") String principalId, @Bind("clientId") String clientId);
}
