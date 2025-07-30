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

import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessControlNotAllowIT extends BaseIT {

  public static String metalakeTestName = RandomNameUtils.genRandomName("test");

  @Test
  public void testNotAllowFilter() {
    GravitinoMetalake metalake =
        client.createMetalake(metalakeTestName, "metalake test", Collections.emptyMap());

    Exception e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.addUser("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.removeUser("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.getUser("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.addGroup("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.getGroup("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.removeGroup("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                metalake.createRole(
                    "test",
                    Collections.emptyMap(),
                    Lists.newArrayList(
                        SecurableObjects.ofMetalake(
                            "test", Lists.newArrayList(Privileges.SelectTable.allow())))));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.getRole("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> metalake.deleteRole("test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> metalake.grantRolesToGroup(Lists.newArrayList("test"), "test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> metalake.grantRolesToUser(Lists.newArrayList("test"), "test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> metalake.revokeRolesFromGroup(Lists.newArrayList("test"), "test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> metalake.revokeRolesFromUser(Lists.newArrayList("test"), "test"));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                metalake.setOwner(
                    MetadataObjects.of(null, "test", MetadataObject.Type.METALAKE),
                    "test",
                    Owner.Type.USER));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    e =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                metalake.getOwner(MetadataObjects.of(null, "test", MetadataObject.Type.METALAKE)));
    Assertions.assertTrue(
        e.getMessage().contains("You should set 'gravitino.authorization.enable'"));

    client.disableMetalake(metalakeTestName);
    client.dropMetalake(metalakeTestName);
  }
}
