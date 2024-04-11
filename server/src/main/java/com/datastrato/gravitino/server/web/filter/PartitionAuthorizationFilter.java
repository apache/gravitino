/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.server.authorization.NameBindings;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;

/**
 * For PartitionAuthorizationFilter, Gravitino only checks whether user is in the metalake.
 * Gravitino allows to handle the privilege issues of the underlying system.
 */
@Provider
@NameBindings.PartitionInterfaces
public class PartitionAuthorizationFilter extends BaseMetalakeUserFilter {
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (!checkUserInMetalake(requestContext)) {
            requestContext.abortWith(
                    Response.status(
                                    SC_FORBIDDEN, "Only the users in the metalake can execute the partition operations")
                            .build());
        }
    }
}
