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
package org.apache.gravitino.s3.credential.webidentity;

import java.io.Closeable;
import java.util.Map;

/**
 * SPI for obtaining a WebIdentity token (an OIDC id_token or access_token) that can be exchanged
 * with AWS STS {@code AssumeRoleWithWebIdentity} for temporary S3 credentials.
 *
 * <p>Implementations decide how the token is sourced (local file, OAuth flow, etc.) and are
 * responsible for any caching and refresh behavior.
 *
 * <p>This SPI is scoped to AWS S3 credential vending (IRSA / {@code AssumeRoleWithWebIdentity}); it
 * is not a generic cross-storage abstraction. Other storage backends (e.g. GCS, Azure) use
 * different workload-identity mechanisms and are out of scope here.
 */
public interface WebIdentityTokenSource extends Closeable {

  /**
   * The configuration name used to select this source via {@code s3-web-identity-token-source}.
   *
   * <p>Must be unique across all implementations on the classpath.
   */
  String name();

  /** Initialize the source with the credential provider's properties. */
  void initialize(Map<String, String> properties);

  /**
   * Returns a current WebIdentity token. Implementations may cache and refresh internally; callers
   * should invoke this for every credential request.
   */
  String getToken();
}
