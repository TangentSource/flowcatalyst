/**
 * Repositories
 *
 * Data access layer for domain entities.
 */

export {
	type PrincipalRepository,
	createPrincipalRepository,
} from './principal-repository.js';

export {
	type ClientRepository,
	createClientRepository,
} from './client-repository.js';

export {
	type AnchorDomainRepository,
	createAnchorDomainRepository,
} from './anchor-domain-repository.js';
