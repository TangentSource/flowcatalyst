package tech.flowcatalyst.serviceaccount.repository;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for ServiceAccount entity.
 */
@RegisterRowMapper(ServiceAccountRowMapper.class)
public interface ServiceAccountDao {

    @SqlQuery("SELECT * FROM service_accounts WHERE id = :id")
    Optional<ServiceAccount> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM service_accounts WHERE code = :code")
    Optional<ServiceAccount> findByCode(@Bind("code") String code);

    @SqlQuery("SELECT * FROM service_accounts WHERE application_id = :applicationId")
    Optional<ServiceAccount> findByApplicationId(@Bind("applicationId") String applicationId);

    @SqlQuery("SELECT * FROM service_accounts WHERE :clientId = ANY(client_ids)")
    List<ServiceAccount> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM service_accounts WHERE active = true ORDER BY name")
    List<ServiceAccount> findActive();

    @SqlQuery("SELECT * FROM service_accounts ORDER BY name")
    List<ServiceAccount> listAll();

    @SqlQuery("SELECT COUNT(*) FROM service_accounts")
    long count();

    @SqlUpdate("""
        INSERT INTO service_accounts (id, code, name, description, client_ids, application_id,
            active, wh_auth_type, wh_auth_token_ref, wh_signing_secret_ref, wh_signing_algorithm,
            wh_credentials_created_at, wh_credentials_regenerated_at, roles, last_used_at, created_at, updated_at)
        VALUES (:id, :code, :name, :description, :clientIds, :applicationId,
            :active, :whAuthType, :whAuthTokenRef, :whSigningSecretRef, :whSigningAlgorithm,
            :whCredentialsCreatedAt, :whCredentialsRegeneratedAt, :roles::jsonb, :lastUsedAt, :createdAt, :updatedAt)
        """)
    void insert(@BindFields ServiceAccount serviceAccount,
                @Bind("clientIds") String[] clientIdsArray,
                @Bind("whAuthType") String whAuthType,
                @Bind("whAuthTokenRef") String whAuthTokenRef,
                @Bind("whSigningSecretRef") String whSigningSecretRef,
                @Bind("whSigningAlgorithm") String whSigningAlgorithm,
                @Bind("whCredentialsCreatedAt") java.time.Instant whCredentialsCreatedAt,
                @Bind("whCredentialsRegeneratedAt") java.time.Instant whCredentialsRegeneratedAt,
                @Bind("roles") String rolesJson);

    @SqlUpdate("""
        UPDATE service_accounts SET code = :code, name = :name, description = :description,
            client_ids = :clientIds, application_id = :applicationId, active = :active,
            wh_auth_type = :whAuthType, wh_auth_token_ref = :whAuthTokenRef,
            wh_signing_secret_ref = :whSigningSecretRef, wh_signing_algorithm = :whSigningAlgorithm,
            wh_credentials_created_at = :whCredentialsCreatedAt, wh_credentials_regenerated_at = :whCredentialsRegeneratedAt,
            roles = :roles::jsonb, last_used_at = :lastUsedAt, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields ServiceAccount serviceAccount,
                @Bind("clientIds") String[] clientIdsArray,
                @Bind("whAuthType") String whAuthType,
                @Bind("whAuthTokenRef") String whAuthTokenRef,
                @Bind("whSigningSecretRef") String whSigningSecretRef,
                @Bind("whSigningAlgorithm") String whSigningAlgorithm,
                @Bind("whCredentialsCreatedAt") java.time.Instant whCredentialsCreatedAt,
                @Bind("whCredentialsRegeneratedAt") java.time.Instant whCredentialsRegeneratedAt,
                @Bind("roles") String rolesJson);

    @SqlUpdate("DELETE FROM service_accounts WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
