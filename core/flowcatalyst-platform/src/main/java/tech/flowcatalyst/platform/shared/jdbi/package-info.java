/**
 * JDBI infrastructure for PostgreSQL database access.
 *
 * <p>This package contains:</p>
 * <ul>
 *   <li>{@link tech.flowcatalyst.platform.shared.jdbi.JdbiProducer} - CDI producer for Jdbi instance</li>
 *   <li>{@link tech.flowcatalyst.platform.shared.jdbi.InstantColumnMapper} - Maps TIMESTAMPTZ to Instant</li>
 *   <li>{@link tech.flowcatalyst.platform.shared.jdbi.InstantArgumentFactory} - Binds Instant to TIMESTAMPTZ</li>
 *   <li>{@link tech.flowcatalyst.platform.shared.jdbi.JsonHelper} - JSON serialization helpers for JSONB columns</li>
 * </ul>
 */
package tech.flowcatalyst.platform.shared.jdbi;
