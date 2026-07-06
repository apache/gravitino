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
package org.apache.gravitino.dto.requests;

import java.util.Collections;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBulkRequests {

  @Test
  public void testDuplicateUserNames() {
    UserNamesRequest request = new UserNamesRequest(new String[] {"user1", "user1"});

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  public void testDuplicateGroupNames() {
    GroupNamesRequest request = new GroupNamesRequest(new String[] {"group1", "group1"});

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  public void testDuplicateRoleNames() {
    RoleNamesRequest request = new RoleNamesRequest(new String[] {"role1", "role1"});

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  @Test
  public void testDuplicateRoleCreateNames() {
    RoleCreateRequest role1 =
        new RoleCreateRequest("role1", Collections.emptyMap(), new SecurableObjectDTO[] {table()});
    RoleCreateRequest role2 =
        new RoleCreateRequest("role1", Collections.emptyMap(), new SecurableObjectDTO[] {table()});
    BulkRoleCreateRequest request =
        new BulkRoleCreateRequest(new RoleCreateRequest[] {role1, role2});

    Assertions.assertThrows(IllegalArgumentException.class, request::validate);
  }

  private SecurableObjectDTO table() {
    return SecurableObjectDTO.builder()
        .withFullName("catalog1.schema1.table1")
        .withType(SecurableObject.Type.TABLE)
        .withPrivileges(
            new PrivilegeDTO[] {
              PrivilegeDTO.builder()
                  .withName(Privilege.Name.SELECT_TABLE)
                  .withCondition(Privilege.Condition.ALLOW)
                  .build()
            })
        .build();
  }
}
