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
package org.apache.gravitino.client.integration.test.authorization;

import org.apache.gravitino.Configs;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PassTroughAuthorizationIT extends BaseRestApiAuthorizationIT {
  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    customConfigs.put(Configs.AUTHORIZATION_IMPL.getKey(), PassThroughAuthorizer.class.getName());
    super.startIntegrationTest();
  }

  @Test
  @Order(1)
  public void testCreateUser() {
    normalUserClient.loadMetalake(METALAKE).addUser("user0");
    client.loadMetalake(METALAKE).addUser("user1");
    client.loadMetalake(METALAKE).addUser("user2");
  }
}
