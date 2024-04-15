/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import org.junit.jupiter.api.Test;

public class TestAccessControlNotAllowedFilter {
  @Test
  public void testAccessControlNotAllowedFilter() throws IOException {
    AccessControlNotAllowedFilter filter = new AccessControlNotAllowedFilter();
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    filter.filter(requestContext);
    verify(requestContext).abortWith(any());
  }
}
