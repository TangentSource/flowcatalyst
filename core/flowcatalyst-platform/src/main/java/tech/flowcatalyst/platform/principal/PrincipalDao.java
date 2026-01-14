package tech.flowcatalyst.platform.principal;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Principal entity.
 */
@RegisterRowMapper(PrincipalRowMapper.class)
public interface PrincipalDao {

    @SqlQuery("SELECT * FROM principals WHERE id = :id")
    Optional<Principal> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM principals WHERE user_identity->>'email' = :email")
    Optional<Principal> findByEmail(@Bind("email") String email);

    @SqlQuery("SELECT * FROM principals WHERE service_account->>'code' = :code")
    Optional<Principal> findByServiceAccountCode(@Bind("code") String code);

    @SqlQuery("SELECT * FROM principals WHERE type = :type")
    List<Principal> findByType(@Bind("type") String type);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId")
    List<Principal> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId AND type = 'USER'")
    List<Principal> findUsersByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId AND type = 'USER' AND active = true")
    List<Principal> findActiveUsersByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId AND type = :type AND active = :active")
    List<Principal> findByClientIdAndTypeAndActive(
            @Bind("clientId") String clientId,
            @Bind("type") String type,
            @Bind("active") Boolean active);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId AND type = :type")
    List<Principal> findByClientIdAndType(
            @Bind("clientId") String clientId,
            @Bind("type") String type);

    @SqlQuery("SELECT * FROM principals WHERE client_id = :clientId AND active = :active")
    List<Principal> findByClientIdAndActive(
            @Bind("clientId") String clientId,
            @Bind("active") Boolean active);

    @SqlQuery("SELECT * FROM principals WHERE active = :active")
    List<Principal> findByActive(@Bind("active") Boolean active);

    @SqlQuery("SELECT * FROM principals")
    List<Principal> listAll();

    @SqlQuery("SELECT * FROM principals WHERE type = 'SERVICE' AND service_account->>'clientId' = :clientId")
    Optional<Principal> findByServiceAccountClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT COUNT(*) FROM principals WHERE user_identity->>'emailDomain' = :domain")
    long countByEmailDomain(@Bind("domain") String domain);

    @SqlUpdate("""
        INSERT INTO principals (id, type, scope, client_id, application_id, name, active,
                               user_identity, service_account, roles, created_at, updated_at)
        VALUES (:id, :type, :scope, :clientId, :applicationId, :name, :active,
                :userIdentity::jsonb, :serviceAccount::jsonb, :roles::jsonb, :createdAt, :updatedAt)
        """)
    void insert(
            @Bind("id") String id,
            @Bind("type") String type,
            @Bind("scope") String scope,
            @Bind("clientId") String clientId,
            @Bind("applicationId") String applicationId,
            @Bind("name") String name,
            @Bind("active") boolean active,
            @Bind("userIdentity") String userIdentityJson,
            @Bind("serviceAccount") String serviceAccountJson,
            @Bind("roles") String rolesJson,
            @Bind("createdAt") Instant createdAt,
            @Bind("updatedAt") Instant updatedAt);

    @SqlUpdate("""
        UPDATE principals SET type = :type, scope = :scope, client_id = :clientId,
               application_id = :applicationId, name = :name, active = :active,
               user_identity = :userIdentity::jsonb, service_account = :serviceAccount::jsonb,
               roles = :roles::jsonb, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(
            @Bind("id") String id,
            @Bind("type") String type,
            @Bind("scope") String scope,
            @Bind("clientId") String clientId,
            @Bind("applicationId") String applicationId,
            @Bind("name") String name,
            @Bind("active") boolean active,
            @Bind("userIdentity") String userIdentityJson,
            @Bind("serviceAccount") String serviceAccountJson,
            @Bind("roles") String rolesJson,
            @Bind("updatedAt") Instant updatedAt);

    @SqlUpdate("DELETE FROM principals WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
