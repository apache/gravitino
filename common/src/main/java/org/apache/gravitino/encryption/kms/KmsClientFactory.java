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
package org.apache.gravitino.encryption.kms;

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Creates a server-side KMS client for one configured provider.
 *
 * <p>The server loads an implementation from {@code gravitino.kms.provider.<name>.className}. The
 * class must have a public no-arg constructor.
 */
@DeveloperApi
public interface KmsClientFactory {

  /**
   * Creates a client bound to a configured KMS provider.
   *
   * <p>Provider credentials are private implementation details of the returned client. They must
   * not be exposed as Gravitino credentials or key properties. The caller owns the returned client
   * and must close it. This method validates configuration and constructs a reusable client without
   * contacting the configured KMS; network and authentication failures are reported by client
   * operations.
   *
   * @param provider logical name of the configured KMS instance
   * @param properties provider-specific configuration
   * @return the configured client
   * @throws IllegalArgumentException if the provider or configuration is invalid
   */
  KmsClient create(String provider, Map<String, String> properties);
}
