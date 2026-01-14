package tech.flowcatalyst.platform.authentication.oauth;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for OAuthClient entity.
 */
@RegisterRowMapper(OAuthClientRowMapper.class)
public interface OAuthClientDao {

    @SqlQuery("SELECT * FROM oauth_clients WHERE id = :id")
    Optional<OAuthClient> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM oauth_clients WHERE client_id = :clientId AND active = true")
    Optional<OAuthClient> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM oauth_clients WHERE client_id = :clientId")
    Optional<OAuthClient> findByClientIdIncludingInactive(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM oauth_clients WHERE :applicationId = ANY(application_ids) AND active = :active ORDER BY client_name")
    List<OAuthClient> findByApplicationIdAndActive(@Bind("applicationId") String applicationId, @Bind("active") boolean active);

    @SqlQuery("SELECT * FROM oauth_clients WHERE :applicationId = ANY(application_ids) ORDER BY client_name")
    List<OAuthClient> findByApplicationId(@Bind("applicationId") String applicationId);

    @SqlQuery("SELECT * FROM oauth_clients WHERE active = :active ORDER BY client_name")
    List<OAuthClient> findByActive(@Bind("active") boolean active);

    @SqlQuery("SELECT * FROM oauth_clients ORDER BY client_name")
    List<OAuthClient> listAll();

    @SqlQuery("SELECT COUNT(*) > 0 FROM oauth_clients WHERE :origin = ANY(allowed_origins) AND active = true")
    boolean isOriginAllowedByAnyClient(@Bind("origin") String origin);

    @SqlUpdate("""
        INSERT INTO oauth_clients (id, client_id, client_name, client_type, client_secret_ref,
            redirect_uris, allowed_origins, grant_types, default_scopes, pkce_required,
            application_ids, service_account_principal_id, active, created_at, updated_at)
        VALUES (:id, :clientId, :clientName, :clientType, :clientSecretRef,
            :redirectUris, :allowedOrigins, :grantTypes, :defaultScopes, :pkceRequired,
            :applicationIds, :serviceAccountPrincipalId, :active, :createdAt, :updatedAt)
        """)
    void insert(@BindBean OAuthClient client,
                @Bind("clientType") String clientType,
                @Bind("redirectUris") String[] redirectUris,
                @Bind("allowedOrigins") String[] allowedOrigins,
                @Bind("grantTypes") String[] grantTypes,
                @Bind("applicationIds") String[] applicationIds);

    @SqlUpdate("""
        UPDATE oauth_clients SET client_id = :clientId, client_name = :clientName,
            client_type = :clientType, client_secret_ref = :clientSecretRef,
            redirect_uris = :redirectUris, allowed_origins = :allowedOrigins,
            grant_types = :grantTypes, default_scopes = :defaultScopes, pkce_required = :pkceRequired,
            application_ids = :applicationIds, service_account_principal_id = :serviceAccountPrincipalId,
            active = :active, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean OAuthClient client,
                @Bind("clientType") String clientType,
                @Bind("redirectUris") String[] redirectUris,
                @Bind("allowedOrigins") String[] allowedOrigins,
                @Bind("grantTypes") String[] grantTypes,
                @Bind("applicationIds") String[] applicationIds);

    @SqlUpdate("DELETE FROM oauth_clients WHERE id = :id")
    int deleteById(@Bind("id") String id);

    @SqlUpdate("DELETE FROM oauth_clients WHERE service_account_principal_id = :principalId")
    long deleteByServiceAccountPrincipalId(@Bind("principalId") String principalId);
}
