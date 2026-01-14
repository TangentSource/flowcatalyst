package tech.flowcatalyst.platform.shared.jdbi;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.jackson2.Jackson2Config;
import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.time.Instant;

/**
 * CDI producer for JDBI instance configured for PostgreSQL.
 * Provides a fully configured Jdbi instance with plugins and custom mappers.
 */
@ApplicationScoped
public class JdbiProducer {

    @Inject
    AgroalDataSource dataSource;

    @Produces
    @ApplicationScoped
    public Jdbi jdbi() {
        Jdbi jdbi = Jdbi.create(dataSource);

        // Install plugins
        jdbi.installPlugin(new SqlObjectPlugin());
        jdbi.installPlugin(new PostgresPlugin());
        jdbi.installPlugin(new Jackson2Plugin());

        // Register Instant column mapper (handles TIMESTAMPTZ)
        jdbi.registerColumnMapper(Instant.class, new InstantColumnMapper());

        // Register Instant argument factory
        jdbi.registerArgument(new InstantArgumentFactory());

        // Configure Jackson for JSONB handling - use JsonHelper's ObjectMapper
        jdbi.getConfig(Jackson2Config.class).setMapper(JsonHelper.getMapper());

        return jdbi;
    }
}
