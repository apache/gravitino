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

package org.apache.gravitino.authorization;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRoleChange {

  @Test
  void testAddSecurableObject() {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    RoleChange.AddSecurableObject change =
        (RoleChange.AddSecurableObject) RoleChange.addSecurableObject("role1", securableObject);

    Assertions.assertEquals(securableObject, change.getSecurableObject());
    Assertions.assertEquals("role1", change.getRoleName());
  }

  @Test
  void testRemoveSecurableObject() {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    RoleChange.RemoveSecurableObject change =
        (RoleChange.RemoveSecurableObject)
            RoleChange.removeSecurableObject("role1", securableObject);

    Assertions.assertEquals("role1", change.getRoleName());
    Assertions.assertEquals(securableObject, change.getSecurableObject());
  }

  @Test
  void testUpdateSecurableObject() {
    SecurableObject oldSecurableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    SecurableObject newSecurableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.deny()));
    SecurableObject diffMetadataSecurableObject =
        SecurableObjects.ofCatalog("other", Lists.newArrayList(Privileges.UseCatalog.deny()));
    SecurableObject samePrivilegeSecurableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    RoleChange.UpdateSecurableObject change =
        (RoleChange.UpdateSecurableObject)
            RoleChange.updateSecurableObject("role1", oldSecurableObject, newSecurableObject);

    Assertions.assertEquals("role1", change.getRoleName());
    Assertions.assertEquals(oldSecurableObject, change.getSecurableObject());
    Assertions.assertEquals(newSecurableObject, change.getNewSecurableObject());

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            RoleChange.updateSecurableObject(
                "role", oldSecurableObject, diffMetadataSecurableObject));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            RoleChange.updateSecurableObject(
                "role", oldSecurableObject, samePrivilegeSecurableObject));
  }

  @Test
  void testEqualsAndHashCode() {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    // AddSecurableObject
    RoleChange change1 = RoleChange.addSecurableObject("role1", securableObject);
    RoleChange change2 = RoleChange.addSecurableObject("role1", securableObject);
    RoleChange change3 = RoleChange.addSecurableObject("role2", securableObject);

    Assertions.assertEquals(change1, change2);
    Assertions.assertEquals(change1.hashCode(), change2.hashCode());
    Assertions.assertNotEquals(change1, change3);
    Assertions.assertNotEquals(change1.hashCode(), change3.hashCode());
    Assertions.assertNotEquals(change2, change3);
    Assertions.assertNotEquals(change2.hashCode(), change3.hashCode());

    // RemoveSecurableObject
    RoleChange change4 = RoleChange.removeSecurableObject("role1", securableObject);
    RoleChange change5 = RoleChange.removeSecurableObject("role1", securableObject);
    RoleChange change6 = RoleChange.removeSecurableObject("role2", securableObject);

    Assertions.assertEquals(change4, change5);
    Assertions.assertEquals(change4.hashCode(), change5.hashCode());
    Assertions.assertNotEquals(change4, change6);
    Assertions.assertNotEquals(change4.hashCode(), change6.hashCode());
    Assertions.assertNotEquals(change5, change6);
    Assertions.assertNotEquals(change5.hashCode(), change6.hashCode());

    // UpdateSecurableObject
    SecurableObject updatedSecurableObject1 =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.deny()));
    SecurableObject updatedSecurableObject2 =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.CreateSchema.deny()));
    RoleChange change7 =
        RoleChange.updateSecurableObject("role1", securableObject, updatedSecurableObject1);
    RoleChange change8 =
        RoleChange.updateSecurableObject("role1", securableObject, updatedSecurableObject1);
    RoleChange change9 =
        RoleChange.updateSecurableObject("role1", securableObject, updatedSecurableObject2);

    Assertions.assertEquals(change7, change8);
    Assertions.assertEquals(change7.hashCode(), change8.hashCode());
    Assertions.assertNotEquals(change7, change9);
    Assertions.assertNotEquals(change7.hashCode(), change9.hashCode());
    Assertions.assertNotEquals(change8, change9);
    Assertions.assertNotEquals(change8.hashCode(), change9.hashCode());
  }
}
