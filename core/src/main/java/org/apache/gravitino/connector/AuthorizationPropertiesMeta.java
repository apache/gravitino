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
package org.apache.gravitino.connector;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class AuthorizationPropertiesMeta {
  /** Ranger admin web URIs */
  public static final String RANGER_ADMIN_URL = "ranger.admin.url";
  /** Ranger authentication type kerberos or simple */
  public static final String RANGER_AUTH_TYPE = "ranger.auth.type";
  /**
   * Ranger admin web login username(auth_type=simple), or kerberos principal(auth_type=kerberos)
   */
  public static final String RANGER_USERNAME = "ranger.username";
  /**
   * Ranger admin web login user password(auth_type=simple), or path of the keytab
   * file(auth_type=kerberos)
   */
  public static final String RANGER_PASSWORD = "ranger.password";
  /** Ranger service name */
  public static final String RANGER_SERVICE_NAME = "ranger.service.name";

  public static final Map<String, PropertyEntry<?>> RANGER_AUTHORIZATION_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              RANGER_SERVICE_NAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_SERVICE_NAME, "The Ranger service name", true, null, false))
          .put(
              RANGER_ADMIN_URL,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_ADMIN_URL, "The Ranger admin web URIs", true, null, false))
          .put(
              RANGER_AUTH_TYPE,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_AUTH_TYPE,
                  "The Ranger admin web auth type (kerberos/simple)",
                  true,
                  null,
                  false))
          .put(
              RANGER_USERNAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_USERNAME, "The Ranger admin web login username", true, null, false))
          .put(
              RANGER_PASSWORD,
              PropertyEntry.stringOptionalPropertyEntry(
                  RANGER_PASSWORD, "The Ranger admin web login password", true, null, false))
          .build();
}
