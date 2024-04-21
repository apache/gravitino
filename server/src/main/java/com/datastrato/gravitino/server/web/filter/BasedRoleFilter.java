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

  SecurableObject getSecurableObject(ContainerRequestContext requestContext);

  String getMetalakeName(ContainerRequestContext requestContext);

  OperationType getOperateType(ContainerRequestContext requestContext);
}
