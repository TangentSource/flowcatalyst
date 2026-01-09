package tech.flowcatalyst.platform.authentication.oauth;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for OAuthClient entities.
 * Provides OAuth client access methods.
 */
public interface OAuthClientRepository {

    // Read operations
    Optional<OAuthClient> findByIdOptional(String id);
    Optional<OAuthClient> findByClientId(String clientId);
    Optional<OAuthClient> findByClientIdIncludingInactive(String clientId);
    List<OAuthClient> findByApplicationIdAndActive(String applicationId, boolean active);
    List<OAuthClient> findByApplicationId(String applicationId);
    List<OAuthClient> findByActive(boolean active);
    List<OAuthClient> listAll();

    /**
     * Check if an origin is allowed by any active OAuth client.
     * Used for CORS preflight validation when client_id is not yet known.
     */
    boolean isOriginAllowedByAnyClient(String origin);

    // Write operations
    void persist(OAuthClient client);
    void update(OAuthClient client);
    void delete(OAuthClient client);
    long deleteByServiceAccountPrincipalId(String principalId);
}
