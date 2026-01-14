package tech.flowcatalyst.dispatchpool;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for DispatchPool entity.
 */
@RegisterRowMapper(DispatchPoolRowMapper.class)
public interface DispatchPoolDao {

    @SqlQuery("SELECT * FROM dispatch_pools WHERE id = :id")
    Optional<DispatchPool> findById(@Bind("id") String id);

    @SqlQuery("""
        SELECT * FROM dispatch_pools
        WHERE code = :code AND (client_id = :clientId OR (client_id IS NULL AND :clientId IS NULL))
        """)
    Optional<DispatchPool> findByCodeAndClientId(@Bind("code") String code, @Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM dispatch_pools WHERE client_id = :clientId ORDER BY code")
    List<DispatchPool> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM dispatch_pools WHERE client_id IS NULL ORDER BY code")
    List<DispatchPool> findAnchorLevel();

    @SqlQuery("SELECT * FROM dispatch_pools WHERE status = :status ORDER BY code")
    List<DispatchPool> findByStatus(@Bind("status") String status);

    @SqlQuery("SELECT * FROM dispatch_pools WHERE status = 'ACTIVE' ORDER BY code")
    List<DispatchPool> findActive();

    @SqlQuery("SELECT * FROM dispatch_pools WHERE status != 'ARCHIVED' ORDER BY code")
    List<DispatchPool> findAllNonArchived();

    @SqlQuery("SELECT * FROM dispatch_pools ORDER BY code")
    List<DispatchPool> listAll();

    @SqlQuery("SELECT COUNT(*) FROM dispatch_pools")
    long count();

    @SqlQuery("""
        SELECT EXISTS(
            SELECT 1 FROM dispatch_pools
            WHERE code = :code AND (client_id = :clientId OR (client_id IS NULL AND :clientId IS NULL))
        )
        """)
    boolean existsByCodeAndClientId(@Bind("code") String code, @Bind("clientId") String clientId);

    @SqlUpdate("""
        INSERT INTO dispatch_pools (id, code, name, description, rate_limit, concurrency, client_id,
                                   client_identifier, status, created_at, updated_at)
        VALUES (:id, :code, :name, :description, :rateLimit, :concurrency, :clientId,
                :clientIdentifier, :status, :createdAt, :updatedAt)
        """)
    void insert(@BindBean DispatchPool pool);

    @SqlUpdate("""
        UPDATE dispatch_pools SET code = :code, name = :name, description = :description,
               rate_limit = :rateLimit, concurrency = :concurrency, client_id = :clientId,
               client_identifier = :clientIdentifier, status = :status, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean DispatchPool pool);

    @SqlUpdate("DELETE FROM dispatch_pools WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
