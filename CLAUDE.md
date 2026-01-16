# FlowCatalyst Project Context

## ID Handling - TSID as Crockford Base32 Strings

**IMPORTANT**: All entity IDs in this project use TSIDs (Time-Sorted IDs) stored and transmitted as Crockford Base32 strings.

### TSID Format
- **Library**: `tsid-creator` (com.github.f4b6a3.tsid)
- **String Format**: 13-character Crockford Base32 (e.g., `0HZXEQ5Y8JY5Z`)
- **Properties**:
  - Lexicographically sortable (newer IDs sort after older ones)
  - URL-safe and case-insensitive
  - Shorter than numeric strings (13 vs ~19 chars)
  - Safe from JavaScript number precision issues

### Rules:
1. **Entities**: Use `String id` for all `@BsonId` fields
2. **Repositories**: Extend `PanacheMongoRepositoryBase<Entity, String>`
3. **DTOs**: All ID fields must be `String` type
4. **Services/Commands**: All ID parameters must be `String` type
5. **Frontend**: IDs are always strings (no parsing needed)

### TsidGenerator Usage:
```java
import tech.flowcatalyst.platform.shared.TsidGenerator;

// Generate a new TSID string
String id = TsidGenerator.generate();  // e.g., "0HZXEQ5Y8JY5Z"

// Convert between formats (for migration/compatibility)
Long longId = TsidGenerator.toLong("0HZXEQ5Y8JY5Z");
String strId = TsidGenerator.toString(786259737685263979L);
```

### Entity Pattern:
```java
@MongoEntity(collection = "my_entities")
public class MyEntity extends PanacheMongoEntityBase {
    @BsonId
    public String id;  // TSID Crockford Base32

    public String relatedEntityId;  // Foreign key as String
}

// Repository
public class MyEntityRepository implements PanacheMongoRepositoryBase<MyEntity, String> {}
```

### Why Not Long?
JavaScript loses precision for integers > 2^53. A TSID like `786259737685263979` becomes `786259737685264000` when parsed as a JavaScript number, causing 404 errors.

## Multi-Tenant Architecture

### UserScope Enum
Users have explicit access scopes:
- `ANCHOR` - Platform admin users, access to all clients
- `PARTNER` - Partner users, access to multiple assigned clients
- `CLIENT` - Users bound to a single client

### Email Domain Configuration
- Anchor domains: Users get `ANCHOR` scope automatically
- Client-bound domains: Users get `CLIENT` scope, constrained to that client
- Unconfigured domains: Default to internal auth

## Authentication

### External Base URL
For OAuth/OIDC callbacks behind a proxy (e.g., Vite dev server), configure:
```properties
flowcatalyst.auth.external-base-url=http://localhost:4200
```

### Token Claims
Session tokens include a `clients` claim:
- `["*"]` for ANCHOR users (access all)
- `["123", "456"]` for specific client IDs

## Database Operations - CRITICAL RULES

### NEVER Drop Collections or Databases Without Permission
**IMPORTANT**: NEVER drop MongoDB collections or databases without explicit user permission. Dropping data is destructive and irreversible.

### Handling Data Type Mismatches
When encountering MongoDB decode errors (e.g., "expected 'DATE_TIME' BsonType but got 'STRING'"), the proper fix is to **migrate the data**, not drop it:

```javascript
// Example: Fix Instant fields stored as STRING instead of DATE_TIME
db.collection.find({ createdAt: { $type: "string" } }).forEach(function(doc) {
  db.collection.updateOne(
    { _id: doc._id },
    { $set: { createdAt: new Date(doc.createdAt) } }
  );
});
```

### Migration vs Dropping
- **Preferred**: Write a migration script to convert incorrect field types
- **Alternative**: Ask user if they want to drop the affected collection
- **Never**: Silently drop collections/databases as a "quick fix"

## TypeScript Error Handling - neverthrow

**IMPORTANT**: Use `neverthrow` for typed error handling in TypeScript code. Do not use try/catch with untyped exceptions for business logic.

### Why neverthrow
- TypeScript exceptions are untyped - `catch (e)` gives you `unknown`
- Result types make error paths explicit in function signatures
- Forces callers to handle errors - can't accidentally ignore them
- Composable with `map`, `mapErr`, `andThen` for clean pipelines

### Basic Patterns
```typescript
import { ok, err, Result, ResultAsync } from 'neverthrow';

// Define typed errors
type ValidationError = { type: 'validation'; field: string; message: string };
type NotFoundError = { type: 'not_found'; id: string };
type NetworkError = { type: 'network'; cause: Error };

// Synchronous functions return Result<T, E>
function parseConfig(json: string): Result<Config, ValidationError> {
  const parsed = JSON.parse(json);
  if (!parsed.name) {
    return err({ type: 'validation', field: 'name', message: 'required' });
  }
  return ok(parsed as Config);
}

// Async functions return ResultAsync<T, E>
function fetchUser(id: string): ResultAsync<User, NotFoundError | NetworkError> {
  return ResultAsync.fromPromise(
    fetch(`/users/${id}`).then(r => r.json()),
    (e) => ({ type: 'network', cause: e as Error })
  ).andThen((data) =>
    data ? ok(data) : err({ type: 'not_found', id })
  );
}

// Handling results
const result = await fetchUser('123');

// Pattern 1: match
result.match(
  (user) => console.log(user.name),
  (error) => {
    if (error.type === 'not_found') console.log('User not found');
    else console.log('Network error', error.cause);
  }
);

// Pattern 2: isOk/isErr guards
if (result.isOk()) {
  console.log(result.value.name);  // TypeScript knows it's User
}

// Pattern 3: unwrapOr for defaults
const user = result.unwrapOr(defaultUser);
```

### Wrapping External Libraries
```typescript
// Wrap throwing functions with fromThrowable
import { fromThrowable } from 'neverthrow';

const safeJsonParse = fromThrowable(
  JSON.parse,
  (e) => ({ type: 'parse_error' as const, cause: e })
);

// Wrap promises with ResultAsync.fromPromise
function safeFetch(url: string): ResultAsync<Response, NetworkError> {
  return ResultAsync.fromPromise(
    fetch(url),
    (e) => ({ type: 'network', cause: e as Error })
  );
}
```

### Combining Results
```typescript
import { Result, ResultAsync, combine, combineWithAllErrors } from 'neverthrow';

// combine - fails fast on first error
const results: Result<number, string>[] = [ok(1), ok(2), ok(3)];
const combined = combine(results);  // Result<number[], string>

// combineWithAllErrors - collects all errors
const allResults = combineWithAllErrors(results);  // Result<number[], string[]>

// Chaining with andThen
fetchUser(id)
  .andThen((user) => fetchOrders(user.id))
  .andThen((orders) => calculateTotal(orders))
  .mapErr((e) => ({ ...e, context: 'order_total' }));
```

### Message Router Error Types
For the message-router, use these standard error types:
```typescript
// Domain errors
type MediationError =
  | { type: 'circuit_open'; name: string }
  | { type: 'timeout'; durationMs: number }
  | { type: 'http_error'; status: number; body?: string }
  | { type: 'network'; cause: Error };

type ProcessingError =
  | { type: 'parse_error'; message: string }
  | { type: 'validation'; field: string }
  | { type: 'rate_limited'; retryAfterMs: number }
  | { type: 'pool_full'; poolCode: string };

type HealthCheckError =
  | { type: 'broker_unreachable'; broker: string; cause: Error }
  | { type: 'queue_not_found'; queueUrl: string }
  | { type: 'auth_failed'; broker: string };
```

### Rules
1. **Business logic**: Always use Result/ResultAsync
2. **Infrastructure boundaries**: Wrap with fromPromise/fromThrowable
3. **Error types**: Define discriminated unions with `type` field
4. **Never throw**: Convert exceptions at boundaries, propagate as Result
5. **Logging**: Log errors at handling site, not at creation site
