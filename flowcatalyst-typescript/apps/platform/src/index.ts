/**
 * FlowCatalyst Platform Service
 *
 * IAM and Eventing service entry point.
 */

import { serve } from '@hono/node-server';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import {
	type FlowCatalystEnv,
	tracingMiddleware,
	auditMiddleware,
	loggingMiddleware,
	executionContextMiddleware,
	createLogger,
	createStandardErrorHandler,
	jsonSuccess,
} from '@flowcatalyst/http';
import {
	createDatabase,
	createTransactionManager,
	createAggregateRegistry,
	createAggregateHandler,
	createDrizzleUnitOfWork,
} from '@flowcatalyst/persistence';
import { getPasswordService } from '@flowcatalyst/platform-crypto';

import { getEnv, isDevelopment } from './env.js';
import { createApi } from './api/index.js';
import {
	createPrincipalRepository,
	createAnchorDomainRepository,
} from './infrastructure/persistence/index.js';
import {
	createCreateUserUseCase,
	createUpdateUserUseCase,
	createActivateUserUseCase,
	createDeactivateUserUseCase,
	createDeleteUserUseCase,
} from './application/index.js';

// Load environment
const env = getEnv();

// Create logger
const logger = createLogger({
	serviceName: 'platform',
	level: env.LOG_LEVEL,
});

logger.info({ env: env.NODE_ENV }, 'Starting FlowCatalyst Platform service');

// Create database connection
const database = createDatabase({ url: env.DATABASE_URL });
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const db = database.db as any;
const transactionManager = createTransactionManager(db);

// Create repositories
const principalRepository = createPrincipalRepository(db);
const anchorDomainRepository = createAnchorDomainRepository(db);

// Create aggregate registry and register handlers
const aggregateRegistry = createAggregateRegistry();
aggregateRegistry.register(createAggregateHandler('Principal', principalRepository));

// Create unit of work
const unitOfWork = createDrizzleUnitOfWork({
	transactionManager,
	aggregateRegistry,
	extractClientId: (aggregate) => {
		if ('clientId' in aggregate && typeof aggregate.clientId === 'string') {
			return aggregate.clientId;
		}
		return null;
	},
});

// Create password service
const passwordService = getPasswordService();

// Create use cases
const createUserUseCase = createCreateUserUseCase({
	principalRepository,
	anchorDomainRepository,
	passwordService,
	unitOfWork,
});

const updateUserUseCase = createUpdateUserUseCase({
	principalRepository,
	unitOfWork,
});

const activateUserUseCase = createActivateUserUseCase({
	principalRepository,
	unitOfWork,
});

const deactivateUserUseCase = createDeactivateUserUseCase({
	principalRepository,
	unitOfWork,
});

const deleteUserUseCase = createDeleteUserUseCase({
	principalRepository,
	unitOfWork,
});

// Create Hono app
const app = new Hono<FlowCatalystEnv>();

// Global middleware
app.use('*', cors());
app.use('*', tracingMiddleware());
app.use(
	'*',
	auditMiddleware({
		validateToken: async () => null, // TODO: Implement token validation in Phase 4
	}),
);
app.use('*', loggingMiddleware(logger));
app.use('*', executionContextMiddleware());

// Error handler
app.onError(createStandardErrorHandler(logger));

// Health check
app.get('/health', (c) => {
	return jsonSuccess(c, {
		status: 'healthy',
		service: 'platform',
		timestamp: new Date().toISOString(),
	});
});

// Mount API routes
app.route(
	'/api',
	createApi({
		principalRepository,
		createUserUseCase,
		updateUserUseCase,
		activateUserUseCase,
		deactivateUserUseCase,
		deleteUserUseCase,
	}),
);

// Start server
const port = env.PORT;
const host = env.HOST;

logger.info({ port, host }, 'Starting HTTP server');

serve(
	{
		fetch: app.fetch,
		port,
		hostname: host,
	},
	(info) => {
		logger.info({ port: info.port, address: info.address }, 'Server started');
		if (isDevelopment()) {
			console.log(`\n  Platform API: http://localhost:${info.port}/api`);
			console.log(`  Health check: http://localhost:${info.port}/health\n`);
		}
	},
);

// Export app for testing
export { app };
