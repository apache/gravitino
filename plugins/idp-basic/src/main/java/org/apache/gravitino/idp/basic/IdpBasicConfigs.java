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
package org.apache.gravitino.idp.basic;

import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/** Configuration keys for the built-in IdP basic authenticator. */
public final class IdpBasicConfigs {

  private IdpBasicConfigs() {}

  /** Default: verified Basic credential caching is off until operators opt in. */
  public static final boolean DEFAULT_VERIFIED_CREDENTIAL_CACHE_ENABLED = false;

  /** Default TTL for successfully verified Basic credentials when the cache is enabled. */
  public static final long DEFAULT_VERIFIED_CREDENTIAL_CACHE_EXPIRATION_SECS = 60L;

  /** Default maximum number of successfully verified Basic credentials to retain. */
  public static final long DEFAULT_VERIFIED_CREDENTIAL_CACHE_MAX_SIZE = 10_000L;

  /**
   * Whether to cache successfully verified Basic credentials between requests.
   *
   * <p>When {@code false} (default), every request re-derives the password hash. Enable this under
   * high-QPS Basic clients after accepting the short revocation window bounded by {@link
   * #VERIFIED_CREDENTIAL_CACHE_EXPIRATION_SECS}.
   */
  public static final ConfigEntry<Boolean> VERIFIED_CREDENTIAL_CACHE_ENABLED =
      new ConfigBuilder("gravitino.idp.basic.verifiedCredentialCacheEnabled")
          .doc(
              "Whether to cache successfully verified Basic credentials so repeat requests can "
                  + "skip password re-derivation. Disabled by default. Failed logins are never "
                  + "cached.")
          .version(ConfigConstants.VERSION_1_3_0)
          .booleanConf()
          .createWithDefault(DEFAULT_VERIFIED_CREDENTIAL_CACHE_ENABLED);

  /**
   * How long a successfully verified Basic credential may skip password re-derivation.
   *
   * <p>Used only when {@link #VERIFIED_CREDENTIAL_CACHE_ENABLED} is {@code true}. Password changes
   * and disables still take effect on the next request because authentication always reloads the
   * user from storage and compares the cached password-hash fingerprint; this TTL bounds cross-node
   * lag and memory lifetime.
   */
  public static final ConfigEntry<Long> VERIFIED_CREDENTIAL_CACHE_EXPIRATION_SECS =
      new ConfigBuilder("gravitino.idp.basic.verifiedCredentialCacheExpirationSecs")
          .doc(
              "Seconds to retain a successfully verified Basic credential before requiring "
                  + "password re-derivation. Used only when verifiedCredentialCacheEnabled is "
                  + "true. Must be > 0 when the cache is enabled.")
          .version(ConfigConstants.VERSION_1_3_0)
          .longConf()
          .checkValue(
              value -> value != null && value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(DEFAULT_VERIFIED_CREDENTIAL_CACHE_EXPIRATION_SECS);

  /** Maximum number of successfully verified Basic credentials retained in memory. */
  public static final ConfigEntry<Long> VERIFIED_CREDENTIAL_CACHE_MAX_SIZE =
      new ConfigBuilder("gravitino.idp.basic.verifiedCredentialCacheMaxSize")
          .doc(
              "Maximum number of successfully verified Basic credentials retained in the "
                  + "in-memory cache. Ignored when verifiedCredentialCacheEnabled is false.")
          .version(ConfigConstants.VERSION_1_3_0)
          .longConf()
          .checkValue(
              value -> value != null && value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(DEFAULT_VERIFIED_CREDENTIAL_CACHE_MAX_SIZE);
}
