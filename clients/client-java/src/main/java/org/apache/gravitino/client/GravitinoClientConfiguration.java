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

package org.apache.gravitino.client;

/** Configuration class for Gravitino Java client */
public class GravitinoClientConfiguration {

  /** An optional http connection timeout in milliseconds. */
  public static final String CLIENT_CONNECTION_TIMEOUT_MS = "gravitino.client.connectionTimeoutMs";

  /** An optional http socket timeout in milliseconds. */
  public static final String CLIENT_SOCKET_TIMEOUT_MS = "gravitino.client.socketTimeoutMs";
}
