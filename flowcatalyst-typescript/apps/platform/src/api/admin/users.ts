/**
 * Users Admin API
 *
 * REST endpoints for user management.
 */

import { Hono } from 'hono';
import { z } from 'zod';
import {
	type FlowCatalystEnv,
	sendResult,
	jsonCreated,
	jsonSuccess,
	noContent,
	notFound,
	badRequest,
	safeValidate,
} from '@flowcatalyst/http';
import { Result } from '@flowcatalyst/application';
import type { UseCase } from '@flowcatalyst/application';

import type {
	CreateUserCommand,
	UpdateUserCommand,
	ActivateUserCommand,
	DeactivateUserCommand,
	DeleteUserCommand,
} from '../../application/index.js';
import type { UserCreated, UserUpdated, UserActivated, UserDeactivated, UserDeleted } from '../../domain/index.js';
import type { PrincipalRepository } from '../../infrastructure/persistence/index.js';

// Request schemas
const CreateUserSchema = z.object({
	email: z.string().email(),
	password: z.string().min(8).nullable(),
	name: z.string().min(1),
	clientId: z.string().length(13).nullable().optional(),
});

const UpdateUserSchema = z.object({
	name: z.string().min(1),
});

// Response schemas for user
interface UserResponse {
	id: string;
	type: string;
	scope: string | null;
	clientId: string | null;
	name: string;
	active: boolean;
	email: string | null;
	emailDomain: string | null;
	idpType: string | null;
	createdAt: string;
	updatedAt: string;
}

interface UsersListResponse {
	users: UserResponse[];
	total: number;
	page: number;
	pageSize: number;
}

/**
 * Dependencies for the users API.
 */
export interface UsersApiDeps {
	readonly principalRepository: PrincipalRepository;
	readonly createUserUseCase: UseCase<CreateUserCommand, UserCreated>;
	readonly updateUserUseCase: UseCase<UpdateUserCommand, UserUpdated>;
	readonly activateUserUseCase: UseCase<ActivateUserCommand, UserActivated>;
	readonly deactivateUserUseCase: UseCase<DeactivateUserCommand, UserDeactivated>;
	readonly deleteUserUseCase: UseCase<DeleteUserCommand, UserDeleted>;
}

/**
 * Create the users admin API routes.
 */
export function createUsersApi(deps: UsersApiDeps): Hono<FlowCatalystEnv> {
	const {
		principalRepository,
		createUserUseCase,
		updateUserUseCase,
		activateUserUseCase,
		deactivateUserUseCase,
		deleteUserUseCase,
	} = deps;

	const app = new Hono<FlowCatalystEnv>();

	// POST /api/admin/users - Create user
	app.post('/', async (c) => {
		const rawBody = await c.req.json();
		const bodyResult = safeValidate(rawBody, CreateUserSchema);
		if (!bodyResult.success) {
			return badRequest(c, bodyResult.error.message);
		}

		const body = bodyResult.data;
		const ctx = c.get('executionContext');

		const command: CreateUserCommand = {
			email: body.email,
			password: body.password,
			name: body.name,
			clientId: body.clientId ?? null,
		};

		const result = await createUserUseCase.execute(command, ctx);

		if (Result.isSuccess(result)) {
			const event = result.value;
			const response: UserResponse = {
				id: event.getData().userId,
				type: 'USER',
				scope: event.getData().scope,
				clientId: event.getData().clientId,
				name: event.getData().name,
				active: true,
				email: event.getData().email,
				emailDomain: event.getData().emailDomain,
				idpType: event.getData().idpType,
				createdAt: event.time.toISOString(),
				updatedAt: event.time.toISOString(),
			};
			return jsonCreated(c, response);
		}

		return sendResult(c, result);
	});

	// GET /api/admin/users - List users
	app.get('/', async (c) => {
		const page = parseInt(c.req.query('page') ?? '0', 10);
		const pageSize = Math.min(parseInt(c.req.query('pageSize') ?? '20', 10), 100);

		const pagedResult = await principalRepository.findPaged(page, pageSize);

		const response: UsersListResponse = {
			users: pagedResult.items
				.filter((p) => p.type === 'USER')
				.map((p) => ({
					id: p.id,
					type: p.type,
					scope: p.scope,
					clientId: p.clientId,
					name: p.name,
					active: p.active,
					email: p.userIdentity?.email ?? null,
					emailDomain: p.userIdentity?.emailDomain ?? null,
					idpType: p.userIdentity?.idpType ?? null,
					createdAt: p.createdAt.toISOString(),
					updatedAt: p.updatedAt.toISOString(),
				})),
			total: pagedResult.totalItems,
			page: pagedResult.page,
			pageSize: pagedResult.pageSize,
		};

		return jsonSuccess(c, response);
	});

	// GET /api/admin/users/:id - Get user by ID
	app.get('/:id', async (c) => {
		const id = c.req.param('id');
		const principal = await principalRepository.findById(id);

		if (!principal || principal.type !== 'USER') {
			return notFound(c, `User not found: ${id}`);
		}

		const response: UserResponse = {
			id: principal.id,
			type: principal.type,
			scope: principal.scope,
			clientId: principal.clientId,
			name: principal.name,
			active: principal.active,
			email: principal.userIdentity?.email ?? null,
			emailDomain: principal.userIdentity?.emailDomain ?? null,
			idpType: principal.userIdentity?.idpType ?? null,
			createdAt: principal.createdAt.toISOString(),
			updatedAt: principal.updatedAt.toISOString(),
		};

		return jsonSuccess(c, response);
	});

	// PUT /api/admin/users/:id - Update user
	app.put('/:id', async (c) => {
		const id = c.req.param('id');
		const rawBody = await c.req.json();
		const bodyResult = safeValidate(rawBody, UpdateUserSchema);
		if (!bodyResult.success) {
			return badRequest(c, bodyResult.error.message);
		}

		const body = bodyResult.data;
		const ctx = c.get('executionContext');

		const command: UpdateUserCommand = {
			userId: id,
			name: body.name,
		};

		const result = await updateUserUseCase.execute(command, ctx);

		if (Result.isSuccess(result)) {
			const principal = await principalRepository.findById(id);
			if (principal) {
				const response: UserResponse = {
					id: principal.id,
					type: principal.type,
					scope: principal.scope,
					clientId: principal.clientId,
					name: principal.name,
					active: principal.active,
					email: principal.userIdentity?.email ?? null,
					emailDomain: principal.userIdentity?.emailDomain ?? null,
					idpType: principal.userIdentity?.idpType ?? null,
					createdAt: principal.createdAt.toISOString(),
					updatedAt: principal.updatedAt.toISOString(),
				};
				return jsonSuccess(c, response);
			}
		}

		return sendResult(c, result);
	});

	// POST /api/admin/users/:id/activate - Activate user
	app.post('/:id/activate', async (c) => {
		const id = c.req.param('id');
		const ctx = c.get('executionContext');

		const command: ActivateUserCommand = {
			userId: id,
		};

		const result = await activateUserUseCase.execute(command, ctx);

		if (Result.isSuccess(result)) {
			const principal = await principalRepository.findById(id);
			if (principal) {
				const response: UserResponse = {
					id: principal.id,
					type: principal.type,
					scope: principal.scope,
					clientId: principal.clientId,
					name: principal.name,
					active: principal.active,
					email: principal.userIdentity?.email ?? null,
					emailDomain: principal.userIdentity?.emailDomain ?? null,
					idpType: principal.userIdentity?.idpType ?? null,
					createdAt: principal.createdAt.toISOString(),
					updatedAt: principal.updatedAt.toISOString(),
				};
				return jsonSuccess(c, response);
			}
		}

		return sendResult(c, result);
	});

	// POST /api/admin/users/:id/deactivate - Deactivate user
	app.post('/:id/deactivate', async (c) => {
		const id = c.req.param('id');
		const ctx = c.get('executionContext');

		const command: DeactivateUserCommand = {
			userId: id,
		};

		const result = await deactivateUserUseCase.execute(command, ctx);

		if (Result.isSuccess(result)) {
			const principal = await principalRepository.findById(id);
			if (principal) {
				const response: UserResponse = {
					id: principal.id,
					type: principal.type,
					scope: principal.scope,
					clientId: principal.clientId,
					name: principal.name,
					active: principal.active,
					email: principal.userIdentity?.email ?? null,
					emailDomain: principal.userIdentity?.emailDomain ?? null,
					idpType: principal.userIdentity?.idpType ?? null,
					createdAt: principal.createdAt.toISOString(),
					updatedAt: principal.updatedAt.toISOString(),
				};
				return jsonSuccess(c, response);
			}
		}

		return sendResult(c, result);
	});

	// DELETE /api/admin/users/:id - Delete user
	app.delete('/:id', async (c) => {
		const id = c.req.param('id');
		const ctx = c.get('executionContext');

		const command: DeleteUserCommand = {
			userId: id,
		};

		const result = await deleteUserUseCase.execute(command, ctx);

		if (Result.isSuccess(result)) {
			return noContent(c);
		}

		return sendResult(c, result);
	});

	return app;
}
