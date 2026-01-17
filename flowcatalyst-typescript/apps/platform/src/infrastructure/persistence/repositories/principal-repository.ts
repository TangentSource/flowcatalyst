/**
 * Principal Repository
 *
 * Data access for Principal aggregates.
 * All data is stored in the principals table with flattened user identity columns
 * and JSONB roles array.
 */

import { eq, and, sql } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import {
	type PaginatedRepository,
	type PagedResult,
	type TransactionContext,
	createPagedResult,
} from '@flowcatalyst/persistence';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyDb = PostgresJsDatabase<any>;

import { principals, type PrincipalRecord, type RoleAssignmentJson } from '../schema/index.js';

import {
	type Principal,
	type NewPrincipal,
	type PrincipalType,
	type UserIdentity,
	type RoleAssignment,
	type UserScope,
	type IdpType,
} from '../../../domain/index.js';

/**
 * Principal repository interface.
 */
export interface PrincipalRepository extends PaginatedRepository<Principal> {
	findByEmail(email: string, tx?: TransactionContext): Promise<Principal | undefined>;
	findByClientId(clientId: string, tx?: TransactionContext): Promise<Principal[]>;
	findByType(type: PrincipalType, tx?: TransactionContext): Promise<Principal[]>;
	findActiveUsersByClientId(clientId: string, tx?: TransactionContext): Promise<Principal[]>;
	existsByEmail(email: string, tx?: TransactionContext): Promise<boolean>;
}

/**
 * Create a Principal repository.
 */
export function createPrincipalRepository(defaultDb: AnyDb): PrincipalRepository {
	const db = (tx?: TransactionContext): AnyDb => (tx?.db as AnyDb) ?? defaultDb;

	return {
		async findById(id: string, tx?: TransactionContext): Promise<Principal | undefined> {
			const [record] = await db(tx)
				.select()
				.from(principals)
				.where(eq(principals.id, id))
				.limit(1);

			if (!record) return undefined;

			return recordToPrincipal(record);
		},

		async findByEmail(email: string, tx?: TransactionContext): Promise<Principal | undefined> {
			const [record] = await db(tx)
				.select()
				.from(principals)
				.where(eq(principals.email, email.toLowerCase()))
				.limit(1);

			if (!record) return undefined;

			return recordToPrincipal(record);
		},

		async findByClientId(clientId: string, tx?: TransactionContext): Promise<Principal[]> {
			const records = await db(tx)
				.select()
				.from(principals)
				.where(eq(principals.clientId, clientId));

			return records.map(recordToPrincipal);
		},

		async findByType(type: PrincipalType, tx?: TransactionContext): Promise<Principal[]> {
			const records = await db(tx)
				.select()
				.from(principals)
				.where(eq(principals.type, type));

			return records.map(recordToPrincipal);
		},

		async findActiveUsersByClientId(clientId: string, tx?: TransactionContext): Promise<Principal[]> {
			const records = await db(tx)
				.select()
				.from(principals)
				.where(
					and(
						eq(principals.clientId, clientId),
						eq(principals.type, 'USER'),
						eq(principals.active, true),
					),
				);

			return records.map(recordToPrincipal);
		},

		async findAll(tx?: TransactionContext): Promise<Principal[]> {
			const records = await db(tx).select().from(principals);
			return records.map(recordToPrincipal);
		},

		async findPaged(page: number, pageSize: number, tx?: TransactionContext): Promise<PagedResult<Principal>> {
			const [countResult] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(principals);
			const totalItems = Number(countResult?.count ?? 0);

			const records = await db(tx)
				.select()
				.from(principals)
				.limit(pageSize)
				.offset(page * pageSize)
				.orderBy(principals.createdAt);

			const items = records.map(recordToPrincipal);
			return createPagedResult(items, page, pageSize, totalItems);
		},

		async count(tx?: TransactionContext): Promise<number> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(principals);
			return Number(result?.count ?? 0);
		},

		async exists(id: string, tx?: TransactionContext): Promise<boolean> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(principals)
				.where(eq(principals.id, id));
			return Number(result?.count ?? 0) > 0;
		},

		async existsByEmail(email: string, tx?: TransactionContext): Promise<boolean> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(principals)
				.where(eq(principals.email, email.toLowerCase()));
			return Number(result?.count ?? 0) > 0;
		},

		async insert(entity: NewPrincipal, tx?: TransactionContext): Promise<Principal> {
			const now = new Date();

			await db(tx).insert(principals).values({
				id: entity.id,
				type: entity.type,
				scope: entity.scope,
				clientId: entity.clientId,
				applicationId: entity.applicationId,
				name: entity.name,
				active: entity.active,
				// Flattened user identity fields
				email: entity.userIdentity?.email ?? null,
				emailDomain: entity.userIdentity?.emailDomain ?? null,
				idpType: entity.userIdentity?.idpType ?? null,
				externalIdpId: entity.userIdentity?.externalIdpId ?? null,
				passwordHash: entity.userIdentity?.passwordHash ?? null,
				lastLoginAt: entity.userIdentity?.lastLoginAt ?? null,
				// Roles as JSONB
				roles: rolesToJson(entity.roles),
				createdAt: entity.createdAt ?? now,
				updatedAt: entity.updatedAt ?? now,
			});

			return this.findById(entity.id, tx) as Promise<Principal>;
		},

		async update(entity: Principal, tx?: TransactionContext): Promise<Principal> {
			const now = new Date();

			await db(tx)
				.update(principals)
				.set({
					type: entity.type,
					scope: entity.scope,
					clientId: entity.clientId,
					applicationId: entity.applicationId,
					name: entity.name,
					active: entity.active,
					// Flattened user identity fields
					email: entity.userIdentity?.email ?? null,
					emailDomain: entity.userIdentity?.emailDomain ?? null,
					idpType: entity.userIdentity?.idpType ?? null,
					externalIdpId: entity.userIdentity?.externalIdpId ?? null,
					passwordHash: entity.userIdentity?.passwordHash ?? null,
					lastLoginAt: entity.userIdentity?.lastLoginAt ?? null,
					// Roles as JSONB
					roles: rolesToJson(entity.roles),
					updatedAt: now,
				})
				.where(eq(principals.id, entity.id));

			return this.findById(entity.id, tx) as Promise<Principal>;
		},

		async persist(entity: NewPrincipal, tx?: TransactionContext): Promise<Principal> {
			const existing = await this.exists(entity.id, tx);
			if (existing) {
				return this.update(entity as Principal, tx);
			}
			return this.insert(entity, tx);
		},

		async deleteById(id: string, tx?: TransactionContext): Promise<boolean> {
			const exists = await this.exists(id, tx);
			if (!exists) return false;
			await db(tx).delete(principals).where(eq(principals.id, id));
			return true;
		},

		async delete(entity: Principal, tx?: TransactionContext): Promise<boolean> {
			return this.deleteById(entity.id, tx);
		},
	};
}

/**
 * Convert a database record to a Principal domain object.
 */
function recordToPrincipal(record: PrincipalRecord): Principal {
	// Build user identity from flat columns if email is present
	let userIdentity: UserIdentity | null = null;
	if (record.email) {
		userIdentity = {
			email: record.email,
			emailDomain: record.emailDomain!,
			idpType: record.idpType as IdpType,
			externalIdpId: record.externalIdpId,
			passwordHash: record.passwordHash,
			lastLoginAt: record.lastLoginAt,
		};
	}

	// Convert JSONB roles to domain objects
	const roles: RoleAssignment[] = jsonToRoles(record.roles);

	return {
		id: record.id,
		type: record.type as PrincipalType,
		scope: record.scope as UserScope | null,
		clientId: record.clientId,
		applicationId: record.applicationId,
		name: record.name,
		active: record.active,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
		userIdentity,
		roles,
	};
}

/**
 * Convert domain role assignments to JSONB format.
 */
function rolesToJson(roles: readonly RoleAssignment[]): RoleAssignmentJson[] {
	return roles.map((role) => ({
		roleName: role.roleName,
		assignmentSource: role.assignmentSource,
		assignedAt: role.assignedAt.toISOString(),
	}));
}

/**
 * Convert JSONB roles to domain role assignments.
 */
function jsonToRoles(json: RoleAssignmentJson[] | null): RoleAssignment[] {
	if (!json) return [];
	return json.map((r) => ({
		roleName: r.roleName,
		assignmentSource: r.assignmentSource,
		assignedAt: new Date(r.assignedAt),
	}));
}
