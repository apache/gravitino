package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.BaseTenantOperations;
import com.datastrato.graviton.schema.AuditInfo;
import com.datastrato.graviton.schema.NameIdentifier;
import com.datastrato.graviton.schema.SchemaVersion;
import com.datastrato.graviton.schema.Tenant;
import com.datastrato.graviton.server.web.Utils;
import java.time.Instant;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/tenants")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TenantOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TenantOperations.class);

  private final BaseTenantOperations ops;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TenantOperations(BaseTenantOperations ops) {
    this.ops = ops;
  }

  @POST
  @Produces("application/vnd.graviton.v1+json")
  public Response create(TenantCreateRequest request) {
    try {
      request.validate();
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to validate create tenant arguments {}", request, e);

      return Utils.illegalArguments(e.getMessage());
    }

    AuditInfo auditInfo =
        new AuditInfo.Builder()
            .withCreator(Utils.remoteUser(httpRequest))
            .withCreateTime(Instant.now())
            .build();

    Tenant.Builder builder =
        new Tenant.Builder()
            .withId(1) // TODO. ID generator should be designed later. @Jerry
            .withName(request.getName())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(auditInfo);
    Optional.ofNullable(request.getComment()).ifPresent(builder::withComment);

    Tenant tenant = builder.build();

    try {
      ops.create(tenant);
      return Utils.ok(new TenantResponse(tenant));

    } catch (Exception e) {
      LOG.error("Failed to create tenant {}", tenant, e);

      return Utils.internalError(e.getMessage());
    }
  }

  // TODO. Are we going to use id or name to get Entity? @Jerry
  @GET
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response get(@PathParam("name") String tenantName) {
    if (tenantName == null || tenantName.isEmpty()) {
      return Utils.illegalArguments("Tenant name is required");
    }

    NameIdentifier identifier = NameIdentifier.parse(tenantName);
    try {
      Tenant tenant = ops.get(identifier);
      if (tenant == null) {
        LOG.warn("Failed to find tenant by name {}", tenantName);
        return Utils.notFound("Failed to find tenant by name " + tenantName);
      } else {
        return Utils.ok(new TenantResponse(tenant));
      }

    } catch (Exception e) {
      LOG.error("Failed to get tenant by name {}", tenantName, e);

      return Utils.internalError(e.getMessage());
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.graviton.v1+json")
  public Response delete(@PathParam("name") String tenantName) {
    if (tenantName == null || tenantName.isEmpty()) {
      return Utils.illegalArguments("Tenant name is required");
    }

    NameIdentifier identifier = NameIdentifier.parse(tenantName);
    try {
      ops.delete(identifier);
      return Utils.ok();

    } catch (Exception e) {
      LOG.error("Failed to delete tenant by name {}", tenantName, e);

      return Utils.internalError(e.getMessage());
    }
  }
}
