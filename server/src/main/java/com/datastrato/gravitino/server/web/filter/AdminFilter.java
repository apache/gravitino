/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
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
@NameBindings.AdminInterface
public class AdminFilter implements BasedRoleFilter {
  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    try {
      AuthorizationUtils.checkPermission(
          getMetalakeName(requestContext), getSecurableObject(requestContext));

    } catch (ForbiddenException fe) {
      requestContext.abortWith(
          Utils.forbidden(
              String.format("Fail to operate [%s] metalake admin", getOperateType(requestContext)),
              fe));
    } catch (IllegalArgumentException ie) {
      requestContext.abortWith(
          Utils.illegalArguments(
              String.format("Fail to operate [%s] metalake admin", getOperateType(requestContext)),
              ie));
    }
  }

  @Override
  public SecurableObject getSecurableObject(ContainerRequestContext requestContext) {
    List<Privilege> privileges = Lists.newArrayList();
    OperationType operationType = getOperateType(requestContext);

    if (operationType.equals(OperationType.ADD)) {
      privileges.add(Privileges.AddUser.allow());
    } else if (operationType.equals(OperationType.REMOVE)) {
      privileges.add(Privileges.RemoveUser.allow());
    }

    return SecurableObjects.ofMetalake(Entity.SYSTEM_METALAKE_RESERVED_NAME, privileges);
  }

  @Override
  public String getMetalakeName(ContainerRequestContext requestContext) {
    return Entity.SYSTEM_METALAKE_RESERVED_NAME;
  }

  @Override
  public OperationType getOperateType(ContainerRequestContext requestContext) {

    if (requestContext.getMethod().equals(POST)) {
      return OperationType.ADD;
    }

    if (requestContext.getMethod().equals(DELETE)) {
      return OperationType.REMOVE;
    }

    throw new IllegalArgumentException(
        String.format("Filter doesn't support %s HTTP method", requestContext.getMethod()));
  }
}
