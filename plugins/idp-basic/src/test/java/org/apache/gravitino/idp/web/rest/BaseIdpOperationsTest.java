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
package org.apache.gravitino.idp.web.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;

/** Base class for built-in IdP REST operation integration tests. */
public abstract class BaseIdpOperationsTest extends JerseyTest {

  /**
   * Registers {@link IdpAuthorizationFilter} with permissive settings so resource tests focus on
   * request handling rather than authorization.
   */
  protected void registerPermissiveIdpAuthorizationFilter(ResourceConfig resourceConfig) {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isServiceAdmin()).thenReturn(true);
    resourceConfig.register(
        new IdpAuthorizationFilter(
            () -> List.of(IdpAuthorizationFilter.BASIC_AUTHENTICATOR), () -> authorizer));
  }
}
