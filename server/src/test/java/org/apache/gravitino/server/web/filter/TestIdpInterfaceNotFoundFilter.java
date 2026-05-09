/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;

public class TestIdpInterfaceNotFoundFilter {

  @Test
  public void testIdpInterfaceNotFoundFilterForIdpPath() throws IOException {
    IdpInterfaceNotFoundFilter filter = new IdpInterfaceNotFoundFilter();
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);

    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath(false)).thenReturn("idp/users/user1");

    filter.filter(requestContext);

    verify(requestContext).abortWith(any());
  }

  @Test
  public void testIdpInterfaceNotFoundFilterForNonIdpPath() throws IOException {
    IdpInterfaceNotFoundFilter filter = new IdpInterfaceNotFoundFilter();
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);

    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath(false)).thenReturn("metalakes/test");

    filter.filter(requestContext);

    verify(requestContext, never()).abortWith(any());
  }
}
