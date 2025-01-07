/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.gravitino.authorization.common;

import java.util.Map;

/**
 * The AuthorizationUserGroupMappingProvider interface defines the public API for mapping Gravitino
 * users and groups to the that in underlying data source system.
 *
 * <p>Typically, the users and group names in Gravitino are the same as the underlying data source.
 * However, in some cases, the user and group names in Gravitino may be different from the
 * underlying data source. For instance, in GCP IAM, the username is the email address or the
 * service account. So the user group mapping provider can be used to map the Gravitino username to
 * the email address or service account.
 */
public interface AuthorizationUserGroupMappingProvider {

  /**
   * Initialize the user group mapping provider with the configuration.
   *
   * @param config The configuration map for the user group mapping provider.
   */
  default void initialize(Map<String, String> config) {}

  /**
   * Get the username from the underlying data source based on the Gravitino username For instance,
   * in GCP IAM, the username is the email address or the service account.
   *
   * @param gravitinoUserName The Gravitino username.
   * @return The username from the underlying data source.
   */
  default String getUserName(String gravitinoUserName) {
    return gravitinoUserName;
  }

  /**
   * Get the group name from the underlying data source based on the Gravitino group name.
   *
   * @param gravitinoGroupName The Gravitino group name.
   * @return The group name from the underlying data source.
   */
  default String getGroupName(String gravitinoGroupName) {
    return gravitinoGroupName;
  }
}
