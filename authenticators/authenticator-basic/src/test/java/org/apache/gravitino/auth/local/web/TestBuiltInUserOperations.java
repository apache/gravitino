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

import java.lang.reflect.Method;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import org.apache.gravitino.auth.local.dto.requests.CreateUserRequest;
import org.apache.gravitino.auth.local.dto.requests.ResetPasswordRequest;
import org.junit.jupiter.api.Test;

public class TestBuiltInUserOperations {

  @Test
  public void testCreateUserRoute() throws NoSuchMethodException {
    Method method = BuiltInUserOperations.class.getMethod("createUser", CreateUserRequest.class);

    assertNull(method.getAnnotation(Path.class));
    assertNotNull(method.getAnnotation(POST.class));
    assertNull(method.getAnnotation(PUT.class));
    assertNull(method.getAnnotation(DELETE.class));
  }

  @Test
  public void testResetPasswordRoute() throws NoSuchMethodException {
    Method method =
        BuiltInUserOperations.class.getMethod(
            "resetPassword", String.class, ResetPasswordRequest.class);

    assertEquals("{user}", method.getAnnotation(Path.class).value());
    assertNotNull(method.getAnnotation(PUT.class));
  }
}
