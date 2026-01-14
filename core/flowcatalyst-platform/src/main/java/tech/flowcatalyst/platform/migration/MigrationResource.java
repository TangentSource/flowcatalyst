package tech.flowcatalyst.platform.migration;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * REST endpoint for triggering MongoDB to PostgreSQL migration.
 *
 * <p>This endpoint is only enabled in dev/test mode for safety.</p>
 *
 * <p>Usage: POST /api/migration/mongo-to-postgres</p>
 */
@Path("/api/migration")
@Produces(MediaType.APPLICATION_JSON)
public class MigrationResource {

    private static final Logger LOG = Logger.getLogger(MigrationResource.class);

    @Inject
    MongoToPostgresMigration migration;

    @ConfigProperty(name = "flowcatalyst.migration.enabled", defaultValue = "false")
    boolean migrationEnabled;

    /**
     * Trigger full migration from MongoDB to PostgreSQL.
     *
     * <p>Requires flowcatalyst.migration.enabled=true to execute.</p>
     */
    @POST
    @Path("/mongo-to-postgres")
    public Response migrateAll() {
        if (!migrationEnabled) {
            LOG.warn("Migration endpoint called but migration is disabled");
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(Map.of(
                            "error", "Migration is disabled",
                            "message", "Set flowcatalyst.migration.enabled=true to enable migration"
                    ))
                    .build();
        }

        try {
            LOG.info("Starting MongoDB to PostgreSQL migration via REST endpoint...");
            long startTime = System.currentTimeMillis();

            migration.migrateAll();

            long duration = System.currentTimeMillis() - startTime;
            LOG.infof("Migration completed in %d ms", duration);

            return Response.ok(Map.of(
                    "status", "success",
                    "message", "Migration completed successfully",
                    "durationMs", duration
            )).build();

        } catch (Exception e) {
            LOG.error("Migration failed", e);
            return Response.serverError()
                    .entity(Map.of(
                            "status", "error",
                            "message", "Migration failed: " + e.getMessage()
                    ))
                    .build();
        }
    }
}
