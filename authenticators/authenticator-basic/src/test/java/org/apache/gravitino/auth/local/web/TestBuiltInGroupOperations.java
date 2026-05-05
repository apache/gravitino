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

package org.apache.gravitino.auth.local.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.gravitino.auth.local.dto.requests.CreateGroupRequest;
import org.apache.gravitino.auth.local.dto.requests.UpdateGroupUsersRequest;
import org.junit.jupiter.api.Test;

public class TestBuiltInGroupOperations {

  @Test
  public void testAddUsersRoute() throws NoSuchMethodException {
    Method method =
        BuiltInGroupOperations.class.getMethod(
            "addUsers", String.class, UpdateGroupUsersRequest.class);

    assertEquals("{group}/add", method.getAnnotation(Path.class).value());
    assertNotNull(method.getAnnotation(PUT.class));
    assertNull(method.getAnnotation(DELETE.class));
  }

  @Test
  public void testCreateGroupRoute() throws NoSuchMethodException {
    Method method = BuiltInGroupOperations.class.getMethod("createGroup", CreateGroupRequest.class);

    assertNull(method.getAnnotation(Path.class));
    assertNotNull(method.getAnnotation(POST.class));
    assertNull(method.getAnnotation(DELETE.class));
  }

  @Test
  public void testDeleteGroupRoute() throws NoSuchMethodException {
    Method method =
        BuiltInGroupOperations.class.getMethod("deleteGroup", String.class, boolean.class);

    assertEquals("{group}", method.getAnnotation(Path.class).value());
    assertNull(method.getAnnotation(PUT.class));
    assertNotNull(method.getAnnotation(DELETE.class));

    Parameter forceParameter = method.getParameters()[1];
    assertEquals("force", forceParameter.getAnnotation(QueryParam.class).value());
    assertEquals("false", forceParameter.getAnnotation(DefaultValue.class).value());
    assertTrue(forceParameter.getType().equals(boolean.class));
  }

  @Test
  public void testRemoveUsersRoute() throws NoSuchMethodException {
    Method method =
        BuiltInGroupOperations.class.getMethod(
            "removeUsers", String.class, UpdateGroupUsersRequest.class, boolean.class);

    assertEquals("{group}/remove", method.getAnnotation(Path.class).value());
    assertNotNull(method.getAnnotation(PUT.class));
    assertNull(method.getAnnotation(DELETE.class));

    Parameter forceParameter = method.getParameters()[2];
    assertEquals("force", forceParameter.getAnnotation(QueryParam.class).value());
    assertEquals("false", forceParameter.getAnnotation(DefaultValue.class).value());
    assertTrue(forceParameter.getType().equals(boolean.class));
  }
}
