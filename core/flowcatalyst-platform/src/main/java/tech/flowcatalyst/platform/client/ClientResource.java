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
import tech.flowcatalyst.platform.authentication.JwtKeyService;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Public API for clients.
 *
 * Returns clients based on the caller's token claims.
 * Service accounts and users can call this endpoint with their access token.
 */
@Path("/api/clients")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "Clients", description = "Client access API")
public class ClientResource {

    @Inject
    ClientRepository clientRepository;

    @Inject
    JwtKeyService jwtKeyService;

    @GET
    @Operation(
        summary = "Get accessible clients",
        description = "Returns the list of clients the authenticated user or service has access to, based on the token's clients claim."
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
    public Response getClients(
        @CookieParam("fc_session") String sessionToken,
        @HeaderParam("Authorization") String authHeader
    ) {
        // Validate token and get principal
        var principalId = jwtKeyService.extractAndValidatePrincipalId(sessionToken, authHeader);
        if (principalId.isEmpty()) {
            return Response.status(401)
                .entity(Map.of("error", "Not authenticated"))
                .build();
        }

        // Extract clients claim from token
        List<String> clientsClaim = jwtKeyService.extractClients(sessionToken, authHeader);

        List<Client> clients;
        if (clientsClaim.contains("*")) {
            // User has access to all clients
            clients = clientRepository.findAllActive();
        } else if (clientsClaim.isEmpty()) {
            // No clients access
            clients = List.of();
        } else {
            // User has access to specific clients
            Set<String> clientIds = new HashSet<>(clientsClaim);
            clients = clientRepository.findByIds(clientIds).stream()
                .filter(c -> c.status == ClientStatus.ACTIVE)
                .toList();
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
    public Response getClient(
        @PathParam("id") String id,
        @CookieParam("fc_session") String sessionToken,
        @HeaderParam("Authorization") String authHeader
    ) {
        // Validate token
        var principalId = jwtKeyService.extractAndValidatePrincipalId(sessionToken, authHeader);
        if (principalId.isEmpty()) {
            return Response.status(401)
                .entity(Map.of("error", "Not authenticated"))
                .build();
        }

        // Check access
        List<String> clientsClaim = jwtKeyService.extractClients(sessionToken, authHeader);
        boolean hasAccess = clientsClaim.contains("*") || clientsClaim.contains(id);
        if (!hasAccess) {
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
