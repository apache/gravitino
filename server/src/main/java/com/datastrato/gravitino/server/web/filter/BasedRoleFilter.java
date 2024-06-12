/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.server.web.rest.OperationType;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

interface BasedRoleFilter extends ContainerRequestFilter {
  String GET = "GET";
  String POST = "POST";
  String PUT = "PUT";
  String DELETE = "DELETE";
  String METALAKE = "metalake";

  /**
   * According to the request information, we return the securable object which the operation need.
   *
   * @param requestContext The request context information.
   * @return The securable object which the operation need.
   */
  SecurableObject getSecurableObject(ContainerRequestContext requestContext);

  /**
   * We use role to check permission. Role is under the metalake. So we should extract the metalake
   * from the request context.
   *
   * @param requestContext The request context information.
   * @return The metalake name.
   */
  String getMetalakeName(ContainerRequestContext requestContext);

  /**
   * According to the request information, we return the operation type.
   *
   * @param requestContext The request context information.
   * @return The operation type.
   */
  OperationType getOperateType(ContainerRequestContext requestContext);
}
