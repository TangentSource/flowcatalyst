/**
 * TypedId - Prefixed ID Serialization
 *
 * Following the Stripe/OpenAI pattern, IDs are serialized as "{prefix}_{tsid}"
 * for external APIs. This provides:
 *
 * - Self-documenting IDs (immediately know the entity type)
 * - Type safety (can't accidentally pass wrong ID type)
 * - Easier debugging and support
 * - Clear API contracts
 *
 * Example: "client_0HZXEQ5Y8JY5Z" instead of just "0HZXEQ5Y8JY5Z"
 */

import { isValid } from './tsid.js';

/**
 * Entity types with their ID prefixes.
 * Matches Java EntityType enum exactly.
 */
export const EntityType = {
	// Core entities
	CLIENT: 'client',
	PRINCIPAL: 'principal',
	APPLICATION: 'app',

	// Authorization
	ROLE: 'role',
	PERMISSION: 'perm',

	// Authentication
	OAUTH_CLIENT: 'oauth',
	AUTH_CODE: 'authcode',

	// Configuration
	CLIENT_AUTH_CONFIG: 'authcfg',
	APP_CLIENT_CONFIG: 'appcfg',
	IDP_ROLE_MAPPING: 'idpmap',
	CORS_ORIGIN: 'cors',
	ANCHOR_DOMAIN: 'anchor',

	// Access management
	CLIENT_ACCESS_GRANT: 'grant',

	// Audit
	AUDIT_LOG: 'audit',

	// Event system
	EVENT_TYPE: 'evtype',
	EVENT: 'event',
	SUBSCRIPTION: 'sub',
	DISPATCH_POOL: 'pool',
	DISPATCH_JOB: 'job',
	SERVICE_ACCOUNT: 'svc',
} as const;

export type EntityTypeKey = keyof typeof EntityType;
export type EntityTypePrefix = (typeof EntityType)[EntityTypeKey];

// Build reverse lookup map
const PREFIX_TO_TYPE: Record<string, EntityTypeKey> = {};
for (const [key, prefix] of Object.entries(EntityType)) {
	PREFIX_TO_TYPE[prefix] = key as EntityTypeKey;
}

/**
 * Reason codes for TypedId errors.
 */
export type TypedIdErrorReason =
	| 'empty' // null/blank input
	| 'missing_separator' // no underscore
	| 'unknown_prefix' // unrecognized prefix
	| 'type_mismatch' // wrong entity type
	| 'invalid_tsid'; // TSID format invalid

/**
 * Error thrown when ID serialization/deserialization fails
 */
export class TypedIdError extends Error {
	constructor(
		message: string,
		public readonly reason: TypedIdErrorReason,
		public readonly expectedType?: EntityTypeKey,
		public readonly actualType?: EntityTypeKey,
		public readonly id?: string,
	) {
		super(message);
		this.name = 'TypedIdError';
	}
}

/**
 * Serialize an internal TSID to external prefixed format.
 *
 * @param type - The entity type
 * @param id - The internal TSID string (13 chars), or null/undefined
 * @returns The external prefixed ID (e.g., "client_0HZXEQ5Y8JY5Z"), or null if id is null/undefined
 */
export function serialize(type: EntityTypeKey, id: string): string;
export function serialize(type: EntityTypeKey, id: null | undefined): null;
export function serialize(type: EntityTypeKey, id: string | null | undefined): string | null;
export function serialize(type: EntityTypeKey, id: string | null | undefined): string | null {
	if (id === null || id === undefined) {
		return null;
	}
	const prefix = EntityType[type];
	return `${prefix}_${id}`;
}

/**
 * Deserialize an external prefixed ID to internal TSID.
 *
 * @param type - The expected entity type
 * @param externalId - The external prefixed ID (e.g., "client_0HZXEQ5Y8JY5Z")
 * @returns The internal TSID string (13 chars)
 * @throws TypedIdError if the ID format is invalid or type doesn't match
 */
export function deserialize(type: EntityTypeKey, externalId: string): string {
	// Check for null/blank input
	if (!externalId || externalId.trim() === '') {
		throw new TypedIdError('ID cannot be null or blank', 'empty', type, undefined, externalId);
	}

	const expectedPrefix = EntityType[type];
	const prefixWithSeparator = `${expectedPrefix}_`;

	if (!externalId.startsWith(prefixWithSeparator)) {
		// Check if it has no separator at all
		if (!externalId.includes('_')) {
			throw new TypedIdError(
				`Invalid ID format: expected prefix '${expectedPrefix}_'`,
				'missing_separator',
				type,
				undefined,
				externalId,
			);
		}

		// Check if it's a different entity type
		const parsed = parseAny(externalId);
		if (parsed.type !== null && parsed.type !== type) {
			throw new TypedIdError(
				`Expected ${type} ID with prefix '${expectedPrefix}_', but got ${parsed.type} ID`,
				'type_mismatch',
				type,
				parsed.type,
				externalId,
			);
		}

		// Has separator but unknown prefix
		throw new TypedIdError(
			`Invalid ID format: unrecognized prefix`,
			'unknown_prefix',
			type,
			undefined,
			externalId,
		);
	}

	const id = externalId.slice(prefixWithSeparator.length);

	if (!isValid(id)) {
		throw new TypedIdError(`Invalid TSID format in ID: ${id}`, 'invalid_tsid', type, undefined, externalId);
	}

	return id;
}

/**
 * Deserialize an external prefixed ID, returning null if invalid.
 *
 * @param type - The expected entity type
 * @param externalId - The external prefixed ID
 * @returns The internal TSID string, or null if invalid
 */
export function deserializeOrNull(type: EntityTypeKey, externalId: string | null | undefined): string | null {
	if (externalId === null || externalId === undefined) {
		return null;
	}

	try {
		return deserialize(type, externalId);
	} catch {
		return null;
	}
}

/**
 * Check if an external ID is valid for the given type.
 *
 * @param type - The expected entity type
 * @param externalId - The external prefixed ID
 * @returns true if valid, false otherwise
 */
export function isValidTypedId(type: EntityTypeKey, externalId: string): boolean {
	try {
		deserialize(type, externalId);
		return true;
	} catch {
		return false;
	}
}

/**
 * Parse any prefixed ID without type validation.
 *
 * @param externalId - The external prefixed ID
 * @returns Object with parsed type and internal ID
 */
export function parseAny(externalId: string): { type: EntityTypeKey | null; id: string } {
	const separatorIndex = externalId.indexOf('_');

	if (separatorIndex === -1) {
		// No prefix, return as-is (might be an internal ID)
		return { type: null, id: externalId };
	}

	const prefix = externalId.slice(0, separatorIndex);
	const id = externalId.slice(separatorIndex + 1);

	const type = PREFIX_TO_TYPE[prefix] ?? null;

	return { type, id };
}

/**
 * Get the prefix for an entity type.
 *
 * @param type - The entity type
 * @returns The prefix string
 */
export function getPrefix(type: EntityTypeKey): EntityTypePrefix {
	return EntityType[type];
}

/**
 * Get the entity type from a prefix.
 *
 * @param prefix - The prefix string
 * @returns The entity type, or null if not found
 */
export function getTypeFromPrefix(prefix: string): EntityTypeKey | null {
	return PREFIX_TO_TYPE[prefix] ?? null;
}

/**
 * Strip the prefix from an ID if present, regardless of type.
 * Useful for accepting both prefixed and unprefixed IDs.
 *
 * @param externalId - The ID (may or may not have prefix)
 * @returns The internal TSID string
 */
export function stripPrefix(externalId: string): string {
	const parsed = parseAny(externalId);
	return parsed.id;
}

/**
 * Ensure an ID has the correct prefix, adding it if missing.
 *
 * @param type - The entity type
 * @param id - The ID (may or may not have prefix)
 * @returns The external prefixed ID
 */
export function ensurePrefix(type: EntityTypeKey, id: string): string {
	const parsed = parseAny(id);

	if (parsed.type === type) {
		// Already has correct prefix
		return id;
	}

	if (parsed.type !== null) {
		// Has wrong prefix - error
		throw new TypedIdError(
			`Cannot add ${type} prefix to ID with ${parsed.type} prefix`,
			'type_mismatch',
			type,
			parsed.type,
			id,
		);
	}

	// No prefix - add it
	return serialize(type, parsed.id)!;
}

/**
 * Serialize multiple internal TSIDs to external prefixed format.
 *
 * @param type - The entity type
 * @param ids - Array of internal TSID strings
 * @returns Array of external prefixed IDs
 */
export function serializeAll(type: EntityTypeKey, ids: string[]): string[] {
	return ids.map((id) => serialize(type, id)!);
}

/**
 * Deserialize multiple external prefixed IDs to internal TSIDs.
 *
 * @param type - The expected entity type
 * @param externalIds - Array of external prefixed IDs
 * @returns Array of internal TSID strings
 * @throws TypedIdError if any ID format is invalid or type doesn't match
 */
export function deserializeAll(type: EntityTypeKey, externalIds: string[]): string[] {
	return externalIds.map((externalId) => deserialize(type, externalId));
}
