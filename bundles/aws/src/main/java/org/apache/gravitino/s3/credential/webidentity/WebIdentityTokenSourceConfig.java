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

/** Property keys shared by {@link WebIdentityTokenSource} implementations. */
public final class WebIdentityTokenSourceConfig {

  /** Selects which {@link WebIdentityTokenSource} implementation to use. */
  public static final String SOURCE = "s3-web-identity-token-source";

  /**
   * Path to a file containing the WebIdentity token. Used by {@link FileWebIdentityTokenSource}.
   */
  public static final String FILE_PATH = "s3-web-identity-token-file";

  /** Default source name used when {@link #SOURCE} is not set. */
  public static final String DEFAULT_SOURCE = FileWebIdentityTokenSource.NAME;

  private WebIdentityTokenSourceConfig() {}
}
