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
package org.apache.gravitino.lineage.auth;

import io.openlineage.client.transports.HttpConfig;
import java.util.Map;

/**
 * Defines an authentication strategy for lineage server communication. Implementations of this
 * interface provide HTTP configuration with appropriate authentication mechanisms for lineage event
 * transport.
 */
public interface LineageServerAuthenticationStrategy {

  /**
   * Configures HTTP transport with authentication parameters for lineage event submission.
   *
   * @param url The target URL of the lineage server endpoint
   * @param configs A map of configuration properties required for authentication.
   * @return Configured {@link HttpConfig} instance with authentication parameters applied
   */
  HttpConfig configureHttpConfig(String url, Map<String, String> configs);
}
