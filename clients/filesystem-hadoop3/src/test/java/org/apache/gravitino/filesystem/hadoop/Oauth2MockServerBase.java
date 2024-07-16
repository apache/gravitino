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
package org.apache.gravitino.filesystem.hadoop;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;

public abstract class Oauth2MockServerBase {
  private static ClientAndServer mockServer;
  private static final String MOCK_SERVER_HOST = "http://127.0.0.1:";
  private static int port;

  @BeforeAll
  public static void setup() {
    mockServer = ClientAndServer.startClientAndServer(0);
    port = mockServer.getLocalPort();
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
  }

  public static String serverUri() {
    return String.format("%s%d", MOCK_SERVER_HOST, port);
  }

  public static ClientAndServer mockServer() {
    return mockServer;
  }
}
