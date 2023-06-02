package com.datastrato.graviton.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastrato.graviton.BaseTenantOperations;
import com.datastrato.graviton.schema.AuditInfo;
import com.datastrato.graviton.schema.SchemaVersion;
import com.datastrato.graviton.schema.Tenant;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTenantOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private BaseTenantOperations tenantOperations = mock(BaseTenantOperations.class);

  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(TenantOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(tenantOperations).to(BaseTenantOperations.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testCreateTenant() {
    TenantCreateRequest req = new TenantCreateRequest("tenant", "comment");

    Response resp =
        target("/tenants")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TenantResponse tenantResponse = resp.readEntity(TenantResponse.class);
    Assertions.assertEquals(0, tenantResponse.getCode());
    Assertions.assertNull(tenantResponse.getMessage());
    Assertions.assertNull(tenantResponse.getType());

    Tenant tenant = tenantResponse.getTenant();
    Assertions.assertEquals("tenant", tenant.getName());
    Assertions.assertEquals("comment", tenant.getComment());

    TenantCreateRequest req1 = new TenantCreateRequest(null, null);
    Response resp1 =
        target("/tenants")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req1, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.INVALID_ARGUMENTS.errorType(), baseResponse.getType());
    Assertions.assertEquals(
        "\"name\" field is required and cannot be empty", baseResponse.getMessage());
  }

  @Test
  public void testGetTenant() {
    String tenantName = "test";
    int id = 1;
    Instant now = Instant.now();
    AuditInfo info = new AuditInfo.Builder().withCreator("graviton").withCreateTime(now).build();
    Tenant tenant =
        new Tenant.Builder()
            .withName(tenantName)
            .withId(id)
            .withAuditInfo(info)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    when(tenantOperations.get(any())).thenReturn(tenant);

    Response resp =
        target("/tenants/" + tenantName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    TenantResponse tenantResponse = resp.readEntity(TenantResponse.class);
    Assertions.assertEquals(0, tenantResponse.getCode());
    Assertions.assertNull(tenantResponse.getMessage());
    Assertions.assertNull(tenantResponse.getType());

    Tenant tenant1 = tenantResponse.getTenant();
    Assertions.assertEquals(tenant, tenant1);

    // Test when specified tenant is not found.
    when(tenantOperations.get(any())).thenReturn(null);

    Response resp1 =
        target("/tenants/" + tenantName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.NOT_FOUND.errorType(), baseResponse.getType());
    Assertions.assertEquals(
        "Failed to find tenant by name " + tenantName, baseResponse.getMessage());

    // Test with internal error
    when(tenantOperations.get(any())).thenThrow(new RuntimeException("Internal error"));

    Response resp2 =
        target("/tenants/" + tenantName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    BaseResponse baseResponse1 = resp2.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse1.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse1.getType());
    Assertions.assertEquals("Internal error", baseResponse1.getMessage());
  }

  @Test
  public void testDeleteTenant() {
    Response resp =
        target("/tenants/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Test throw an exception when deleting tenant.
    doThrow(new RuntimeException("Internal error")).when(tenantOperations).delete(any());

    Response resp1 =
        target("/tenants/test")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp1.getStatus());

    BaseResponse baseResponse = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorCode(), baseResponse.getCode());
    Assertions.assertEquals(ErrorType.INTERNAL_ERROR.errorType(), baseResponse.getType());
    Assertions.assertEquals("Internal error", baseResponse.getMessage());
  }
}
