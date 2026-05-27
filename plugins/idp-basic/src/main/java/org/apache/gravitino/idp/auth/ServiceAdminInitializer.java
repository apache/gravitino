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

package org.apache.gravitino.idp.auth;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.IdpUserGroupManager;

/** Initializes configured service admins in the built-in IdP during server startup. */
public final class ServiceAdminInitializer {

  /** Environment variable for the initial password of configured service admins. */
  public static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";

  private ServiceAdminInitializer() {}

  /**
   * Initialize the service admins using the current runtime environment.
   *
   * @param config The configuration object to initialize the service admins.
   */
  public static void initialize(Config config) throws IOException {
    GravitinoEnv env = GravitinoEnv.getInstance();
    IdpUserGroupManager manager = IdpUserGroupManager.getInstance(config, env.idGenerator());
    manager.initializeConfiguredServiceAdmins(
        config, StringUtils.defaultString(System.getenv(INITIAL_ADMIN_PASSWORD_ENV)));
  }
}
