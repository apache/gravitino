/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web.filter;

import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.server.authorization.NameBindings;
import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * AccessControlNotAllowedFilter is used for filter the requests related to access control if
 * Gravitino doesn't enable authorization. The filter return 405 error code. You can refer to
 * https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405. No methods will be returned in the
 * allow methods.
 */
@Provider
@NameBindings.AccessControlInterfaces
public class AccessControlNotAllowedFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    requestContext.abortWith(
        Response.status(
                SC_METHOD_NOT_ALLOWED,
                String.format(
                    "You should set '%s' to true in the server side `gravitino.conf`"
                        + " to enable the authorization of the system, otherwise these interfaces can't work.",
                    Configs.ENABLE_AUTHORIZATION.getKey()))
            .allow(Collections.emptySet())
            .build());
  }
}
