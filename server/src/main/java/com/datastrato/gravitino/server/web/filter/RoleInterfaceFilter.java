/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

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
@NameBindings.RoleInterface
public class RoleInterfaceFilter implements BasedRoleFilter {
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    try {
      AuthorizationUtils.checkPermission(
          getMetalakeName(requestContext), getSecurableObject(requestContext));

    } catch (ForbiddenException fe) {
      requestContext.abortWith(
          Utils.forbidden(
              String.format("Fail to operate [%s] role", getOperateType(requestContext)), fe));
    } catch (IllegalArgumentException ie) {
      requestContext.abortWith(
          Utils.illegalArguments(
              String.format("Fail to operate [%s] role", getOperateType(requestContext)), ie));
    }
  }

  @Override
  public SecurableObject getSecurableObject(ContainerRequestContext requestContext) {
    List<Privilege> privileges = Lists.newArrayList();
    if (getOperateType(requestContext).equals(OperationType.CREATE)) {
      privileges.add(Privileges.CreateRole.allow());
    } else if (getOperateType(requestContext).equals(OperationType.DELETE)) {
      privileges.add(Privileges.DeleteRole.allow());
    } else if (getOperateType(requestContext).equals(OperationType.GET)) {
      privileges.add(Privileges.GetRole.allow());
    }

    return SecurableObjects.ofMetalake(getMetalakeName(requestContext), privileges);
  }

  @Override
  public String getMetalakeName(ContainerRequestContext requestContext) {
    return requestContext.getUriInfo().getPathParameters().getFirst(METALAKE);
  }

  @Override
  public OperationType getOperateType(ContainerRequestContext requestContext) {
    if (requestContext.getMethod().equals(POST)) {
      return OperationType.CREATE;
    }

    if (requestContext.getMethod().equals(GET)) {
      return OperationType.GET;
    }

    if (requestContext.getMethod().equals(DELETE)) {
      return OperationType.DELETE;
    }

    throw new IllegalArgumentException(
        String.format("Filter doesn't support %s HTTP method", requestContext.getMethod()));
  }
}
