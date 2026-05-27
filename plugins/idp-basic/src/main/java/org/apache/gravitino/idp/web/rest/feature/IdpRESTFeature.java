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
package org.apache.gravitino.idp.web.rest.feature;

import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.auth.ServiceAdminInitializer;
import org.apache.gravitino.idp.web.rest.IdpAuthorizationFilter;
import org.apache.gravitino.idp.web.rest.IdpBasicBinder;
import org.apache.gravitino.idp.web.rest.IdpGroupOperations;
import org.apache.gravitino.idp.web.rest.IdpUserOperations;

/**
 * Conditionally registers built-in IdP REST resources when {@code basic} is configured in {@link
 * Configs#AUTHENTICATORS}.
 *
 * <p>Configure {@link Configs#REST_API_EXTENSION_PACKAGES} to {@code
 * org.apache.gravitino.idp.web.rest.feature} so Jersey only auto-discovers this feature. IdP REST
 * resource classes remain in {@code org.apache.gravitino.idp.web.rest} and are registered here only
 * when the {@code basic} authenticator is enabled. Also initializes configured service admins in
 * the built-in IdP when they do not yet exist.
 */
@Provider
public class IdpRESTFeature implements Feature {

  /** Authenticator name that enables built-in IdP management APIs. */
  public static final String BASIC_AUTHENTICATOR = "basic";

  @Override
  public boolean configure(FeatureContext context) {
    Config config = GravitinoEnv.getInstance().config();
    if (!basicAuthenticatorEnabled(config.get(Configs.AUTHENTICATORS))) {
      return true;
    }

    try {
      ServiceAdminInitializer.initialize(config);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to initialize built-in IdP service admins", e);
    }

    context.register(IdpBasicBinder.class);
    context.register(IdpUserOperations.class);
    context.register(IdpGroupOperations.class);
    context.register(IdpAuthorizationFilter.class);
    return true;
  }

  /**
   * Returns whether built-in IdP management REST APIs should be registered.
   *
   * @param authenticators configured authenticator names
   * @return {@code true} when {@code basic} is included in {@code authenticators}
   */
  public static boolean basicAuthenticatorEnabled(List<String> authenticators) {
    return authenticators != null && authenticators.contains(BASIC_AUTHENTICATOR);
  }
}
