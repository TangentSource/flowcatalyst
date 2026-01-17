/**
 * Database Schema
 *
 * All table definitions for the platform service.
 */

// Re-export common utilities from persistence package
export { tsidColumn, timestampColumn, baseEntityColumns, type BaseEntity, type NewEntity } from '@flowcatalyst/persistence';

// Re-export core tables from persistence package
export { events, auditLogs } from '@flowcatalyst/persistence';

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
