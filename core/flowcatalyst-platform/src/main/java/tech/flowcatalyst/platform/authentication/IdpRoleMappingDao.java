package tech.flowcatalyst.platform.authentication;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Optional;

/**
 * JDBI DAO interface for IdpRoleMapping entity.
 */
@RegisterRowMapper(IdpRoleMappingRowMapper.class)
public interface IdpRoleMappingDao {

    @SqlQuery("SELECT * FROM idp_role_mappings WHERE idp_role_name = :idpRoleName")
    Optional<IdpRoleMapping> findByIdpRoleName(@Bind("idpRoleName") String idpRoleName);

    @SqlUpdate("""
        INSERT INTO idp_role_mappings (id, idp_role_name, internal_role_name, created_at)
        VALUES (:id, :idpRoleName, :internalRoleName, :createdAt)
        """)
    void insert(@BindFields IdpRoleMapping mapping);

    @SqlUpdate("DELETE FROM idp_role_mappings WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
