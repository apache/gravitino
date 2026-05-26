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

package org.apache.gravitino.idp.web;

import javax.ws.rs.core.Response;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpRestUtils {

  @Test
  public void testHandleExceptionMapsKnownErrors() {
    Response notFound =
        IdpRestUtils.handleException(
            "user", IdpOperationType.GET, "alice", new NotFoundException("missing"));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), notFound.getStatus());

    Response alreadyExists =
        IdpRestUtils.handleException(
            "group", IdpOperationType.ADD, "engineering", new AlreadyExistsException("exists"));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), alreadyExists.getStatus());

    Response illegalArguments =
        IdpRestUtils.handleException(
            "user", IdpOperationType.ADD, "alice", new IllegalArgumentException("invalid"));
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), illegalArguments.getStatus());
  }
}
