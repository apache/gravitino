package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.authorization.AuthorizationUtils;
import com.datastrato.gravitino.authorization.Privilege;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.server.web.rest.OperationType;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.ext.Provider;

@Provider
@NameBindings.MetalakeInterface
public class MetalakeInterfaceFilter implements BasedRoleFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    try {
      AuthorizationUtils.checkPermission(
          getMetalakeName(requestContext), getSecurableObject(requestContext));

    } catch (ForbiddenException fe) {
      requestContext.abortWith(
          Utils.forbidden(
              String.format("Fail to operate [%s] metalake", getOperateType(requestContext)), fe));
    } catch (IllegalArgumentException ie) {
      requestContext.abortWith(
          Utils.illegalArguments(
              String.format("Fail to operate [%s] metalake", getOperateType(requestContext)), ie));
    }
  }

  @Override
  public SecurableObject getSecurableObject(ContainerRequestContext requestContext) {

    List<Privilege> privileges = Lists.newArrayList();
    OperationType operationType = getOperateType(requestContext);
    if (operationType.equals(OperationType.CREATE)) {
      privileges.add(Privileges.CreateMetalake.allow());
    } else if (operationType.equals(OperationType.LOAD)) {
      privileges.add(Privileges.UseMetalake.allow());
    } else if (operationType.equals(OperationType.ALTER)) {
      privileges.add(Privileges.ManageMetalake.allow());
    } else if (operationType.equals(OperationType.DROP)) {
      privileges.add(Privileges.ManageMetalake.allow());
    }

    String metalake = requestContext.getUriInfo().getPathParameters().getFirst(METALAKE);
    if (metalake == null) {
      return SecurableObjects.ofAllMetalakes(privileges);
    }

    return null;
  }

  @Override
  public String getMetalakeName(ContainerRequestContext requestContext) {
    String metalake = requestContext.getUriInfo().getPathParameters().getFirst(METALAKE);

    if (metalake == null) {
      return Entity.SYSTEM_METALAKE_RESERVED_NAME;
    }

    return metalake;
  }

  @Override
  public OperationType getOperateType(ContainerRequestContext requestContext) {
    if (requestContext.getMethod().equals(POST)) {
      return OperationType.CREATE;
    }

    if (requestContext.getMethod().equals(GET)) {
      String metalake = requestContext.getUriInfo().getPathParameters().getFirst(METALAKE);
      if (metalake != null) {
        return OperationType.LOAD;
      } else {
        return OperationType.LIST;
      }
    }

    if (requestContext.getMethod().equals(DELETE)) {
      return OperationType.DROP;
    }

    if (requestContext.getMethod().equals(PUT)) {
      return OperationType.ALTER;
    }

    throw new IllegalArgumentException(
        String.format("Filter doesn't support %s HTTP method", requestContext.getMethod()));
  }
}
