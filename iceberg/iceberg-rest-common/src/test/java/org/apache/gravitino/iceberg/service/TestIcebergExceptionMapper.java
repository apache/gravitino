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
package org.apache.gravitino.iceberg.service;

import javax.ws.rs.core.Response;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergExceptionMapper {
  private final IcebergExceptionMapper icebergExceptionMapper = new IcebergExceptionMapper();

  private void checkExceptionStatus(Exception exception, int statusCode) {
    Response response = icebergExceptionMapper.toResponse(exception);
    Assertions.assertEquals(statusCode, response.getStatus());
  }

  @Test
  public void testIcebergExceptionMapper() {
    checkExceptionStatus(new IllegalArgumentException(""), 400);
    checkExceptionStatus(new ValidationException(""), 400);
    checkExceptionStatus(new NamespaceNotEmptyException(""), 400);
    checkExceptionStatus(new NotAuthorizedException(""), 401);
    checkExceptionStatus(new ForbiddenException(""), 403);
    checkExceptionStatus(new NoSuchNamespaceException(""), 404);
    checkExceptionStatus(new NoSuchTableException(""), 404);
    checkExceptionStatus(new NoSuchIcebergTableException(""), 404);
    checkExceptionStatus(new UnsupportedOperationException(""), 406);
    checkExceptionStatus(new AlreadyExistsException(""), 409);
    checkExceptionStatus(new CommitFailedException(""), 409);
    checkExceptionStatus(new UnprocessableEntityException(""), 422);
    checkExceptionStatus(new CommitStateUnknownException("", new RuntimeException()), 500);
    checkExceptionStatus(new ServiceUnavailableException(""), 503);
    checkExceptionStatus(new RuntimeException(), 500);
  }
}
