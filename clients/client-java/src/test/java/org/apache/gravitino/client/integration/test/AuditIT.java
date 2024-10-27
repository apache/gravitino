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

package org.apache.gravitino.client.integration.test;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuditIT extends BaseIT {

  private static final String expectUser = System.getProperty("user.name");

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    super.startIntegrationTest();
  }

  @Test
  public void testAuditMetalake() {
    String metalakeAuditName = RandomNameUtils.genRandomName("metalakeAudit");
    String newName = RandomNameUtils.genRandomName("newmetaname");

    GravitinoMetalake metaLake =
        client.createMetalake(metalakeAuditName, "metalake A comment", Collections.emptyMap());
    Assertions.assertEquals(expectUser, metaLake.auditInfo().creator());
    Assertions.assertNull(metaLake.auditInfo().lastModifier());
    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
        };
    metaLake = client.alterMetalake(metalakeAuditName, changes);
    Assertions.assertEquals(expectUser, metaLake.auditInfo().creator());
    Assertions.assertEquals(expectUser, metaLake.auditInfo().lastModifier());
    Assertions.assertDoesNotThrow(() -> client.disableMetalake(newName));
    Assertions.assertTrue(client.dropMetalake(newName), "metaLake should be dropped");
    Assertions.assertFalse(client.dropMetalake(newName), "metalake should be non-existent");
  }
}
