/**
 * OAuth Client Repository
 *
 * Data access for OAuthClient entities.
 */

import { eq, sql } from 'drizzle-orm';
import type { PostgresJsDatabase } from 'drizzle-orm/postgres-js';
import type { TransactionContext } from '@flowcatalyst/persistence';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyDb = PostgresJsDatabase<any>;

import {
	oauthClients,
	type OAuthClientRecord,
	type NewOAuthClientRecord,
} from '../schema/index.js';
import {
	type OAuthClient,
	type NewOAuthClient,
	type OAuthClientType,
	type OAuthGrantType,
} from '../../../domain/index.js';

/**
 * OAuth client repository interface.
 */
export interface OAuthClientRepository {
	findById(id: string, tx?: TransactionContext): Promise<OAuthClient | undefined>;
	findByClientId(clientId: string, tx?: TransactionContext): Promise<OAuthClient | undefined>;
	findAll(tx?: TransactionContext): Promise<OAuthClient[]>;
	findActive(tx?: TransactionContext): Promise<OAuthClient[]>;
	count(tx?: TransactionContext): Promise<number>;
	exists(id: string, tx?: TransactionContext): Promise<boolean>;
	existsByClientId(clientId: string, tx?: TransactionContext): Promise<boolean>;
	insert(entity: NewOAuthClient, tx?: TransactionContext): Promise<OAuthClient>;
	update(entity: OAuthClient, tx?: TransactionContext): Promise<OAuthClient>;
	persist(entity: NewOAuthClient, tx?: TransactionContext): Promise<OAuthClient>;
	deleteById(id: string, tx?: TransactionContext): Promise<boolean>;
	delete(entity: OAuthClient, tx?: TransactionContext): Promise<boolean>;
}

/**
 * Create an OAuthClient repository.
 */
export function createOAuthClientRepository(defaultDb: AnyDb): OAuthClientRepository {
	const db = (tx?: TransactionContext): AnyDb => (tx?.db as AnyDb) ?? defaultDb;

	return {
		async findById(id: string, tx?: TransactionContext): Promise<OAuthClient | undefined> {
			const [record] = await db(tx)
				.select()
				.from(oauthClients)
				.where(eq(oauthClients.id, id))
				.limit(1);

			if (!record) return undefined;

			return recordToOAuthClient(record);
		},

		async findByClientId(clientId: string, tx?: TransactionContext): Promise<OAuthClient | undefined> {
			const [record] = await db(tx)
				.select()
				.from(oauthClients)
				.where(eq(oauthClients.clientId, clientId))
				.limit(1);

			if (!record) return undefined;

			return recordToOAuthClient(record);
		},

		async findAll(tx?: TransactionContext): Promise<OAuthClient[]> {
			const records = await db(tx).select().from(oauthClients);
			return records.map(recordToOAuthClient);
		},

		async findActive(tx?: TransactionContext): Promise<OAuthClient[]> {
			const records = await db(tx)
				.select()
				.from(oauthClients)
				.where(eq(oauthClients.active, true));
			return records.map(recordToOAuthClient);
		},

		async count(tx?: TransactionContext): Promise<number> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(oauthClients);
			return Number(result?.count ?? 0);
		},

		async exists(id: string, tx?: TransactionContext): Promise<boolean> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(oauthClients)
				.where(eq(oauthClients.id, id));
			return Number(result?.count ?? 0) > 0;
		},

		async existsByClientId(clientId: string, tx?: TransactionContext): Promise<boolean> {
			const [result] = await db(tx)
				.select({ count: sql<number>`count(*)` })
				.from(oauthClients)
				.where(eq(oauthClients.clientId, clientId));
			return Number(result?.count ?? 0) > 0;
		},

		async insert(entity: NewOAuthClient, tx?: TransactionContext): Promise<OAuthClient> {
			const now = new Date();
			const record: NewOAuthClientRecord = {
				id: entity.id,
				clientId: entity.clientId,
				clientName: entity.clientName,
				clientType: entity.clientType,
				clientSecretRef: entity.clientSecretRef,
				redirectUris: [...entity.redirectUris],
				allowedOrigins: [...entity.allowedOrigins],
				grantTypes: [...entity.grantTypes],
				defaultScopes: entity.defaultScopes,
				pkceRequired: entity.pkceRequired,
				applicationIds: [...entity.applicationIds],
				serviceAccountPrincipalId: entity.serviceAccountPrincipalId,
				active: entity.active,
				createdAt: entity.createdAt ?? now,
				updatedAt: entity.updatedAt ?? now,
			};

			await db(tx).insert(oauthClients).values(record);

			return this.findById(entity.id, tx) as Promise<OAuthClient>;
		},

		async update(entity: OAuthClient, tx?: TransactionContext): Promise<OAuthClient> {
			const now = new Date();
			await db(tx)
				.update(oauthClients)
				.set({
					clientName: entity.clientName,
					clientType: entity.clientType,
					clientSecretRef: entity.clientSecretRef,
					redirectUris: [...entity.redirectUris],
					allowedOrigins: [...entity.allowedOrigins],
					grantTypes: [...entity.grantTypes],
					defaultScopes: entity.defaultScopes,
					pkceRequired: entity.pkceRequired,
					applicationIds: [...entity.applicationIds],
					serviceAccountPrincipalId: entity.serviceAccountPrincipalId,
					active: entity.active,
					updatedAt: now,
				})
				.where(eq(oauthClients.id, entity.id));

			return this.findById(entity.id, tx) as Promise<OAuthClient>;
		},

		async persist(entity: NewOAuthClient, tx?: TransactionContext): Promise<OAuthClient> {
			const existing = await this.exists(entity.id, tx);
			if (existing) {
				return this.update(entity as OAuthClient, tx);
			}
			return this.insert(entity, tx);
		},

		async deleteById(id: string, tx?: TransactionContext): Promise<boolean> {
			const exists = await this.exists(id, tx);
			if (!exists) return false;
			await db(tx).delete(oauthClients).where(eq(oauthClients.id, id));
			return true;
		},

		async delete(entity: OAuthClient, tx?: TransactionContext): Promise<boolean> {
			return this.deleteById(entity.id, tx);
		},
	};
}

/**
 * Convert a database record to an OAuthClient.
 */
function recordToOAuthClient(record: OAuthClientRecord): OAuthClient {
	return {
		id: record.id,
		clientId: record.clientId,
		clientName: record.clientName,
		clientType: record.clientType as OAuthClientType,
		clientSecretRef: record.clientSecretRef,
		redirectUris: record.redirectUris ?? [],
		allowedOrigins: record.allowedOrigins ?? [],
		grantTypes: (record.grantTypes ?? []) as OAuthGrantType[],
		defaultScopes: record.defaultScopes,
		pkceRequired: record.pkceRequired,
		applicationIds: record.applicationIds ?? [],
		serviceAccountPrincipalId: record.serviceAccountPrincipalId,
		active: record.active,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
	};
}
