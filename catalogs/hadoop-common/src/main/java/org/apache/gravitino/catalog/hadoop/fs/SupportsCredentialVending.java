/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.hadoop.fs;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.credential.Credential;

/** Interface for file systems that support credential vending. */
public interface SupportsCredentialVending {
  /**
   * Get the configuration needed for the file system credential based on the credentials.
   *
   * @param credentials the credentials to be used for the file system
   * @return the configuration for the file system credential
   */
  default Map<String, String> getFileSystemCredentialConf(Credential[] credentials) {
    return ImmutableMap.of();
  }

  /**
   * Whether the given configuration already contains client-provided static storage credentials
   * (e.g. {@code s3-access-key-id}/{@code s3-secret-access-key}). When the client configures its
   * own credentials, they must take precedence over server-side vended credentials, so callers
   * should skip credential vending in that case.
   *
   * @param config the merged file system configuration
   * @return true if the client supplied its own storage credentials, false otherwise
   */
  default boolean containsClientCredentials(Map<String, String> config) {
    return false;
  }

  /**
   * Returns whether all of the given keys are present in the configuration with a non-blank value.
   *
   * @param config the configuration to inspect
   * @param keys the keys that must all be present and non-blank
   * @return true if every key is present with a non-blank value, false otherwise
   */
  static boolean allNonBlank(Map<String, String> config, String... keys) {
    if (config == null) {
      return false;
    }
    for (String key : keys) {
      String value = config.get(key);
      if (value == null || value.trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
