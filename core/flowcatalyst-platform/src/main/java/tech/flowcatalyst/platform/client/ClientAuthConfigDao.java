package tech.flowcatalyst.platform.client;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for ClientAuthConfig entity.
 */
@RegisterRowMapper(ClientAuthConfigRowMapper.class)
public interface ClientAuthConfigDao {

    @SqlQuery("SELECT * FROM client_auth_configs WHERE id = :id")
    Optional<ClientAuthConfig> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM client_auth_configs WHERE email_domain = :emailDomain")
    Optional<ClientAuthConfig> findByEmailDomain(@Bind("emailDomain") String emailDomain);

    @SqlQuery("""
        SELECT * FROM client_auth_configs
        WHERE primary_client_id = :clientId
           OR client_id = :clientId
           OR :clientId = ANY(additional_client_ids)
           OR :clientId = ANY(granted_client_ids)
        ORDER BY email_domain
        """)
    List<ClientAuthConfig> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM client_auth_configs WHERE config_type = :configType ORDER BY email_domain")
    List<ClientAuthConfig> findByConfigType(@Bind("configType") String configType);

    @SqlQuery("SELECT * FROM client_auth_configs ORDER BY email_domain")
    List<ClientAuthConfig> listAll();

    @SqlQuery("SELECT COUNT(*) > 0 FROM client_auth_configs WHERE email_domain = :emailDomain")
    boolean existsByEmailDomain(@Bind("emailDomain") String emailDomain);

    @SqlUpdate("""
        INSERT INTO client_auth_configs (id, email_domain, config_type, client_id, primary_client_id,
            additional_client_ids, granted_client_ids, auth_provider, oidc_issuer_url, oidc_client_id,
            oidc_client_secret_ref, oidc_multi_tenant, oidc_issuer_pattern, created_at, updated_at)
        VALUES (:id, :emailDomain, :configType, :clientId, :primaryClientId,
            :additionalClientIds, :grantedClientIds, :authProvider, :oidcIssuerUrl, :oidcClientId,
            :oidcClientSecretRef, :oidcMultiTenant, :oidcIssuerPattern, :createdAt, :updatedAt)
        """)
    void insert(@BindBean ClientAuthConfig config,
                @Bind("configType") String configType,
                @Bind("authProvider") String authProvider,
                @Bind("additionalClientIds") String[] additionalClientIds,
                @Bind("grantedClientIds") String[] grantedClientIds);

    @SqlUpdate("""
        UPDATE client_auth_configs SET email_domain = :emailDomain, config_type = :configType,
            client_id = :clientId, primary_client_id = :primaryClientId,
            additional_client_ids = :additionalClientIds, granted_client_ids = :grantedClientIds,
            auth_provider = :authProvider, oidc_issuer_url = :oidcIssuerUrl, oidc_client_id = :oidcClientId,
            oidc_client_secret_ref = :oidcClientSecretRef, oidc_multi_tenant = :oidcMultiTenant,
            oidc_issuer_pattern = :oidcIssuerPattern, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean ClientAuthConfig config,
                @Bind("configType") String configType,
                @Bind("authProvider") String authProvider,
                @Bind("additionalClientIds") String[] additionalClientIds,
                @Bind("grantedClientIds") String[] grantedClientIds);

    @SqlUpdate("DELETE FROM client_auth_configs WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
