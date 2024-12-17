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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import java.util.Map;

/** The properties for Ranger authorization plugin. */
public class RangerAuthorizationProperties {
  /** Ranger admin web URIs */
  public static final String RANGER_ADMIN_URL = "authorization.ranger.admin.url";

  /** Ranger service type */
  public static final String RANGER_SERVICE_TYPE = "authorization.ranger.service.type";

  /** Ranger service name */
  public static final String RANGER_SERVICE_NAME = "authorization.ranger.service.name";

  /** Ranger authentication type kerberos or simple */
  public static final String RANGER_AUTH_TYPE = "authorization.ranger.auth.type";

  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  public static final String RANGER_USERNAME = "authorization.ranger.username";

  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  public static final String RANGER_PASSWORD = "authorization.ranger.password";

  public static void validate(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(RANGER_ADMIN_URL),
        String.format("%s is required", RANGER_ADMIN_URL));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_SERVICE_TYPE),
        String.format("%s is required", RANGER_SERVICE_TYPE));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_SERVICE_NAME),
        String.format("%s is required", RANGER_SERVICE_NAME));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_AUTH_TYPE),
        String.format("%s is required", RANGER_AUTH_TYPE));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_USERNAME), String.format("%s is required", RANGER_USERNAME));
    Preconditions.checkArgument(
        properties.containsKey(RANGER_PASSWORD), String.format("%s is required", RANGER_PASSWORD));
    Preconditions.checkArgument(
        properties.get(RANGER_ADMIN_URL) != null,
        String.format("%s is required", RANGER_ADMIN_URL));
    Preconditions.checkArgument(
        properties.get(RANGER_SERVICE_NAME) != null,
        String.format("%s is required", RANGER_SERVICE_NAME));
    Preconditions.checkArgument(
        properties.get(RANGER_AUTH_TYPE) != null,
        String.format("%s is required", RANGER_AUTH_TYPE));
    Preconditions.checkArgument(
        properties.get(RANGER_USERNAME) != null, String.format("%s is required", RANGER_USERNAME));
    Preconditions.checkArgument(
        properties.get(RANGER_PASSWORD) != null, String.format("%s is required", RANGER_PASSWORD));
  }
}
