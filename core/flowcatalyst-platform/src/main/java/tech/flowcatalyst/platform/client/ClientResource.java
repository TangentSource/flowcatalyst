package tech.flowcatalyst.platform.client;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import tech.flowcatalyst.platform.audit.AuditContext;

import java.util.List;
import java.util.Map;

/**
 * Public API for clients.
 *
 * Returns clients based on the caller's database-stored permissions.
 * Authorization is determined by the principal's scope (ANCHOR, PARTNER, CLIENT).
 */
@Path("/api/clients")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Clients", description = "Client access API")
public class ClientResource {

    @Inject
    ClientRepository clientRepository;

    @Inject
    AuditContext auditContext;

    @GET
    @Operation(
        summary = "Get accessible clients",
        description = "Returns the list of clients the authenticated principal has access to, based on their scope."
    )
    @APIResponses({
        @APIResponse(
            responseCode = "200",
            description = "List of accessible clients",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = ClientListResponse.class))
        ),
        @APIResponse(responseCode = "401", description = "Not authenticated")
    })
    public Response getClients() {
        // Require authentication (throws 401 if not authenticated)
        auditContext.requirePrincipalId();

        List<Client> clients;
        if (auditContext.hasAccessToAllClients()) {
            // ANCHOR scope: access to all clients
            clients = clientRepository.findAllActive();
        } else {
            // CLIENT/PARTNER scope: access to home client only (for now)
            // TODO: For PARTNER scope, also include clients from partner_client_access table
            clients = auditContext.getHomeClientId()
                .flatMap(clientRepository::findByIdOptional)
                .filter(c -> c.status == ClientStatus.ACTIVE)
                .map(List::of)
                .orElse(List.of());
        }

        List<ClientResponse> responses = clients.stream()
            .map(ClientResponse::from)
            .toList();

        return Response.ok(new ClientListResponse(responses)).build();
    }

    @GET
    @Path("/{id}")
    @Operation(
        summary = "Get client by ID",
        description = "Returns a specific client if the caller has access to it."
    )
    @APIResponses({
        @APIResponse(responseCode = "200", description = "Client found"),
        @APIResponse(responseCode = "401", description = "Not authenticated"),
        @APIResponse(responseCode = "403", description = "Access denied"),
        @APIResponse(responseCode = "404", description = "Client not found")
    })
    public Response getClient(@PathParam("id") String id) {
        // Require authentication (throws 401 if not authenticated)
        auditContext.requirePrincipalId();

        // Check access based on principal's scope
        if (!auditContext.hasAccessToClient(id)) {
            return Response.status(403)
                .entity(Map.of("error", "Access denied to this client"))
                .build();
        }

        // Find client
        return clientRepository.findByIdOptional(id)
            .filter(c -> c.status == ClientStatus.ACTIVE)
            .map(c -> Response.ok(ClientResponse.from(c)).build())
            .orElse(Response.status(404)
                .entity(Map.of("error", "Client not found"))
                .build());
    }

    // ========================================================================
    // DTOs
    // ========================================================================

    public record ClientListResponse(List<ClientResponse> items) {}

    public record ClientResponse(
        String id,
        String name,
        String identifier,
        String status
    ) {
        public static ClientResponse from(Client client) {
            return new ClientResponse(
                client.id,
                client.name,
                client.identifier,
                client.status != null ? client.status.name() : null
            );
        }
    }
}
