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
package org.apache.gravitino.storage.relational.po;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link ExtendedGroupPO} equality. */
public class TestExtendedGroupPO {

  private static ExtendedGroupPO po(String groupName, String roleNames, String roleIds)
      throws IllegalAccessException {
    ExtendedGroupPO po = new ExtendedGroupPO();
    FieldUtils.writeField(po, "groupName", groupName, true);
    FieldUtils.writeField(po, "roleNames", roleNames, true);
    FieldUtils.writeField(po, "roleIds", roleIds, true);
    return po;
  }

  @Test
  void testEqualsIncludesGroupFields() throws IllegalAccessException {
    // Two different groups with identical role strings must not be equal; the
    // parent GroupPO fields (groupId/groupName/metalakeId/...) are part of identity.
    Assertions.assertNotEquals(po("group-a", "r1,r2", "1,2"), po("group-b", "r1,r2", "1,2"));
  }

  @Test
  void testEqualsAndHashCodeConsistent() throws IllegalAccessException {
    ExtendedGroupPO po1 = po("group-a", "r1,r2", "1,2");
    ExtendedGroupPO po2 = po("group-a", "r1,r2", "1,2");
    Assertions.assertEquals(po1, po2);
    Assertions.assertEquals(po1.hashCode(), po2.hashCode());
  }
}
