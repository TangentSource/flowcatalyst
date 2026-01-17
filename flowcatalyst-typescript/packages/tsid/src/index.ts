/**
 * @flowcatalyst/tsid
 *
 * TSID (Time-Sorted ID) generation and TypedId serialization.
 *
 * @example
 * ```typescript
 * import { generate, Tsid, EntityType, serialize, deserialize } from '@flowcatalyst/tsid';
 *
 * // Generate a new TSID
 * const id = generate(); // "0HZXEQ5Y8JY5Z"
 *
 * // Serialize for external API
 * const externalId = serialize('CLIENT', id); // "client_0HZXEQ5Y8JY5Z"
 *
 * // Deserialize from external API
 * const internalId = deserialize('CLIENT', externalId); // "0HZXEQ5Y8JY5Z"
 *
 * // Work with TSID object
 * const tsid = Tsid.from(id);
 * console.log(tsid.getDate()); // Creation timestamp
 * ```
 */

// TSID generation
export { Tsid, generate, toBigInt, fromBigInt, isValid, getTimestamp } from './tsid.js';

// TypedId serialization
export {
	EntityType,
	type EntityTypeKey,
	type EntityTypePrefix,
	type TypedIdErrorReason,
	TypedIdError,
	serialize,
	serializeAll,
	deserialize,
	deserializeAll,
	deserializeOrNull,
	isValidTypedId,
	parseAny,
	getPrefix,
	getTypeFromPrefix,
	stripPrefix,
	ensurePrefix,
} from './typed-id.js';
