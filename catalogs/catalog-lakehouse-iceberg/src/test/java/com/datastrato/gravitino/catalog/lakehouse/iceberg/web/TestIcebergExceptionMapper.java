/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web;

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
  void testIcebergExceptionMapper() {
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
