/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.credential;

/**
 * Marks a provider that issues dynamic credentials without exposing stored source credentials.
 *
 * <p>Catalog credential requests containing only providers with this capability require metadata
 * access without static-secret privileges. Implementations must not return stored passwords, access
 * keys, or other source credentials. Dynamic credentials must respect the authorization context;
 * providers requiring storage paths must return no credentials for a pathless catalog request.
 *
 * <p>Providers without this capability retain the existing catalog secret-privilege checks,
 * including implementations compiled against an earlier version of the credential provider SPI.
 */
public interface SupportsDynamicCredential extends CredentialProvider {}
