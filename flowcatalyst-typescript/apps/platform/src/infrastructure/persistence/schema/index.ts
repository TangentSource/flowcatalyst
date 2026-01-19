/**
 * Database Schema
 *
 * All table definitions for the platform service.
 */

// Re-export common utilities from persistence package
export { tsidColumn, timestampColumn, baseEntityColumns, type BaseEntity, type NewEntity } from '@flowcatalyst/persistence';

// Re-export core tables from persistence package
export { events, auditLogs, type AuditLogRecord, type NewAuditLog } from '@flowcatalyst/persistence';

// Principal tables
export {
	principals,
	type RoleAssignmentJson,
	type ServiceAccountJson,
	type PrincipalRecord,
	type NewPrincipalRecord,
} from './principals.js';

// Client tables
export {
	clients,
	type ClientNoteJson,
	type ClientRecord,
	type NewClientRecord,
} from './clients.js';

// Anchor domain tables
export {
	anchorDomains,
	type AnchorDomainRecord,
	type NewAnchorDomainRecord,
} from './anchor-domains.js';

// Application tables
export {
	applications,
	applicationClientConfigs,
	type ApplicationRecord,
	type NewApplicationRecord,
	type ApplicationClientConfigRecord,
	type NewApplicationClientConfigRecord,
} from './applications.js';

// Role tables
export {
	authRoles,
	authPermissions,
	type AuthRoleRecord,
	type NewAuthRoleRecord,
	type AuthPermissionRecord,
	type NewAuthPermissionRecord,
} from './roles.js';

// Client access grant tables
export {
	clientAccessGrants,
	type ClientAccessGrantRecord,
	type NewClientAccessGrantRecord,
} from './client-access-grants.js';

// Client auth config tables
export {
	clientAuthConfigs,
	type ClientAuthConfigRecord,
	type NewClientAuthConfigRecord,
} from './client-auth-configs.js';

// OAuth client tables
export {
	oauthClients,
	type OAuthClientRecord,
	type NewOAuthClientRecord,
} from './oauth-clients.js';
