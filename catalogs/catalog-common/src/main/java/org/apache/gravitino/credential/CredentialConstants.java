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
 */

package org.apache.gravitino.credential;

public class CredentialConstants {
  /**
   * @deprecated Please use {@link #CREDENTIAL_PROVIDERS} instead.
   */
  @Deprecated public static final String CREDENTIAL_PROVIDER_TYPE = "credential-provider-type";

  public static final String CREDENTIAL_PROVIDERS = "credential-providers";
  public static final String CREDENTIAL_CACHE_EXPIRE_RATIO = "credential-cache-expire-ratio";
  public static final String CREDENTIAL_CACHE_MAX_SIZE = "credential-cache-max-size";
  public static final String S3_TOKEN_EXPIRE_IN_SECS = "s3-token-expire-in-secs";

  /**
   * Whether the vended {@code s3:ListBucket} statement also allows the bare location prefix. When
   * {@code true}, a directory-root {@code getFileStatus} HEAD returns 404 instead of 403, at the
   * cost of allowing enumeration of sibling keys that share the location's string prefix. Defaults
   * to {@code false} (secure); fileset catalogs enable it because access goes through the Hadoop
   * FileSystem API.
   */
  public static final String S3_CREDENTIAL_LIST_LOCATION_PREFIX =
      "s3-credential-list-location-prefix";

  public static final String OSS_TOKEN_EXPIRE_IN_SECS = "oss-token-expire-in-secs";
  public static final String ADLS_TOKEN_EXPIRE_IN_SECS = "adls-token-expire-in-secs";

  /** The HTTP header used to get the credential from fileset location */
  public static final String HTTP_HEADER_CURRENT_LOCATION_NAME = "Current-Location-Name";

  private CredentialConstants() {}
}
